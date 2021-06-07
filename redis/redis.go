package redis

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/lambda-direct/gocast-stock-calc/data"
	"io/fs"
	"io/ioutil"
	"log"
	"sort"
	"strconv"
	"sync"
)

type Client struct {
	rdb *redis.Client
}

func New(ctx context.Context) (*Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})

	err := rdb.Ping(ctx).Err()
	if err != nil {
		return nil, err
	}

	return &Client{rdb}, nil
}

func (c *Client) Close() error {
	return c.rdb.Close()
}

func (c *Client) PutDataPoint(ctx context.Context, p *data.Point) error {
	return c.rdb.HSet(ctx, "data", p.Timestamp, p.Rate).Err()
}

func (c *Client) SyncData(ctx context.Context, file fs.File) (data.Set, error) {
	hasher := sha256.New()

	s, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("unable to read file: %w", err)
	}

	hasher.Write(s)
	newhash := hex.EncodeToString(hasher.Sum(nil))

	oldhash, err := c.rdb.Get(ctx, "datahash").Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("unable to get datahash key: %w", err)
	}

	if oldhash != newhash {
		log.Println("Dataset changed")

		var ds data.Set
		if err := json.Unmarshal(s, &ds); err != nil {
			return nil, fmt.Errorf("unable to unmarshall dataset from redis: %w", err)
		}

		var wg sync.WaitGroup
		wg.Add(len(ds))

		for _, p := range ds {
			func(p data.Point) {
				defer wg.Done()
				c.rdb.HSet(ctx, "data", p.Timestamp, p.Rate)
			}(p)
		}

		wg.Wait()

		return ds, nil
	}

	log.Println("retrieving data from cache")

	dataMap, err := c.rdb.HGetAll(ctx, "data").Result()
	if err != nil {
		return nil, err
	}

	ds := make(data.Set, len(dataMap))

	i := 0
	for timestampStr, rateStr := range dataMap {
		rate, err := strconv.ParseFloat(rateStr, 64)
		if err != nil {
			panic(fmt.Errorf("unable to parse %s as float64: %w", rateStr, err))
		}

		timestamp, err := strconv.ParseUint(timestampStr, 10, 64)
		if err != nil {
			panic(fmt.Errorf("unable to parse %s as uint64: %w", timestampStr, err))
		}

		ds[i] = data.Point{
			Rate:      rate,
			Timestamp: timestamp,
		}

		i++
	}

	sort.Slice(ds, func(i, j int) bool {
		return ds[i].Timestamp < ds[j].Timestamp
	})

	return ds, nil
}
