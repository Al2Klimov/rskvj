package main

import (
	"context"
	"encoding"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/tjarratt/babble"
	"math/rand"
	"net"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"
)

var types = map[reflect.Type]struct {
	generator func() interface{}
	extractor interface{}
}{
	reflect.TypeOf(""): {
		func() interface{} { return uuid4().String() },
		func(src string, dest *string) { *dest = src },
	},
	reflect.TypeOf(uint16(0)): {
		func() interface{} { return rand.Uint64() % (1 << 16) },
		func(src string, dest *uint16) {
			u, errPU := strconv.ParseUint(src, 10, 64)
			assert(errPU, "Couldn't parse uInt", log.Fields{"raw": src})
			*dest = uint16(u)
		},
	},
	reflect.TypeOf(uuid.UUID{}): {
		func() interface{} { return uuid4() },
		func(src string, dest encoding.TextUnmarshaler) {
			assert(dest.UnmarshalText([]byte(src)), "Couldn't parse UUID", log.Fields{"raw": src})
		},
	},
}

func main() {
	rHost := flag.String("redis-host", "localhost", "HOST")
	rPort := flag.Uint("redis-port", 6379, "PORT")
	rPass := flag.String("redis-pass", "", "PASSWORD")
	nFields := flag.Uint("fields", 16, "AMOUNT")
	items := flag.Uint("items", 10000, "AMOUNT")

	flag.Parse()

	rd := redis.NewClient(&redis.Options{
		Addr:        net.JoinHostPort(*rHost, strconv.FormatUint(uint64(*rPort), 10)),
		Password:    *rPass,
		ReadTimeout: 30 * time.Second,
	})

	babbler := babble.NewBabbler()
	babbler.Count = 3
	babbler.Separator = "_"

	oTypes := make([]reflect.Type, 0, len(types))
	fields := make([]reflect.StructField, 0, *nFields)
	jsonTags := make(map[string]int, *nFields)

	for t := range types {
		oTypes = append(oTypes, t)
	}

	for i := uint(0); i < *nFields; i++ {
		jsonTag := strings.ToLower(babbler.Babble())

		fields = append(fields, reflect.StructField{
			Name: strings.ReplaceAll(strings.Title(strings.ReplaceAll(jsonTag, "_", " ")), " ", ""),
			Type: oTypes[rand.Intn(len(oTypes))],
			Tag:  reflect.StructTag(`json:"` + jsonTag + `"`),
		})

		jsonTags[jsonTag] = int(i)
	}

	typ := reflect.StructOf(fields)
	log.Infof("Type: %s", typ)

	{
		stream := uuid4().String()
		log.Info("Filling JSON...")

		for i := uint(0); i < *items; i++ {
			item := make(map[string]interface{}, len(jsonTags))
			for k, i := range jsonTags {
				item[k] = types[typ.Field(i).Type].generator()
			}

			jsn, errJM := json.Marshal(item)
			assert(errJM, "Couldn't encode JSON", log.Fields{"raw": item})

			_, errXA := rd.XAdd(context.Background(), &redis.XAddArgs{
				Stream: stream,
				Values: []string{"json", string(jsn)},
			}).Result()
			assert(errXA, "XADD failed", log.Fields{"stream": stream})

			fmt.Fprintf(os.Stderr, "%d/%d\r", i, *items)
		}

		log.Info("Reading JSON...")
		i := uint(0)
		start := time.Now()

		xra := &redis.XReadArgs{
			Streams: []string{stream, "0-0"},
			Count:   100,
		}

	XRead:
		for {
			streams, errXR := rd.XRead(context.Background(), xra).Result()
			if errXR == redis.Nil {
				break
			}

			assert(errXR, "XREAD failed", log.Fields{"stream": stream})

			for _, s := range streams {
				for _, msg := range s.Messages {
					xra.Streams[1] = msg.ID
					jsn := msg.Values["json"]

					if js, ok := jsn.(string); ok {
						assert(
							json.Unmarshal([]byte(js), reflect.New(typ).Interface()),
							"Couldn't decode JSON", log.Fields{"raw": js},
						)

						fmt.Fprintf(os.Stderr, "%d/%d\r", i, *items)
						i++

						if i == *items {
							break XRead
						}
					}
				}
			}
		}

		log.WithFields(log.Fields{"took": time.Now().Sub(start)}).Info("Done")
		log.Info("Cleaning up JSON...")

		_, errDl := rd.Del(context.Background(), stream).Result()
		assert(errDl, "DEL failed", log.Fields{"key": stream})
	}

	stream := uuid4().String()
	log.Info("Filling K/V...")

	for i := uint(0); i < *items; i++ {
		item := make([]string, 0, len(jsonTags)*2)
		for k, i := range jsonTags {
			item = append(item, k, fmt.Sprintf("%v", types[typ.Field(i).Type].generator()))
		}

		_, errXA := rd.XAdd(context.Background(), &redis.XAddArgs{
			Stream: stream,
			Values: item,
		}).Result()
		assert(errXA, "XADD failed", log.Fields{"stream": stream})

		fmt.Fprintf(os.Stderr, "%d/%d\r", i, *items)
	}

	type extractor struct {
		field     int
		extractor reflect.Value
	}

	extractors := make(map[string]extractor, len(jsonTags))
	for k, i := range jsonTags {
		extractors[k] = extractor{i, reflect.ValueOf(types[typ.Field(i).Type].extractor)}
	}

	log.Info("Reading K/V...")
	i := uint(0)
	start := time.Now()

	xra := &redis.XReadArgs{
		Streams: []string{stream, "0-0"},
		Count:   100,
	}

XRead2:
	for {
		streams, errXR := rd.XRead(context.Background(), xra).Result()
		if errXR == redis.Nil {
			break
		}

		assert(errXR, "XREAD failed", log.Fields{"stream": stream})

		for _, s := range streams {
			for _, msg := range s.Messages {
				xra.Streams[1] = msg.ID
				z := reflect.New(typ).Elem()

				for k, v := range msg.Values {
					if vs, ok := v.(string); ok {
						ex := extractors[k]
						ex.extractor.Call([]reflect.Value{reflect.ValueOf(vs), z.Field(ex.field).Addr()})

						fmt.Fprintf(os.Stderr, "%d/%d\r", i, *items)
						i++

						if i == *items {
							break XRead2
						}
					}
				}
			}
		}
	}

	log.WithFields(log.Fields{"took": time.Now().Sub(start)}).Info("Done")
	log.Info("Cleaning up K/V...")

	_, errDl := rd.Del(context.Background(), stream).Result()
	assert(errDl, "DEL failed", log.Fields{"key": stream})
}

func uuid4() uuid.UUID {
	uid, errNR := uuid.NewRandom()
	assert(errNR, "Couldn't generate UUIDv4", nil)
	return uid
}

func assert(err error, message string, fields log.Fields) {
	if err != nil {
		log.WithFields(fields).WithFields(log.Fields{"error": err.Error()}).Fatal(message)
	}
}
