package main

import (
	"context"
	"github.com/lambda-direct/gocast-stock-calc/pg"
	"github.com/lambda-direct/gocast-stock-calc/util"
	"log"
	"math"
	"os"
	"sync"
	gotime "time"

	"github.com/lambda-direct/gocast-stock-calc/data"
	"github.com/lambda-direct/gocast-stock-calc/time"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pgClient, err := pg.New(ctx)
	util.PanicOnError(err)

	defer pgClient.Close()

	file, err := os.Open("data.json")
	util.PanicOnError(err)

	ds, err := pgClient.SyncData(ctx, file)
	util.PanicOnError(err)

	initialTime := ds[0].Timestamp

	ds5min := ds.SliceByTime(initialTime - time.New(5, gotime.Minute).To(gotime.Millisecond))
	ds30min := ds.SliceByTime(initialTime - time.New(30, gotime.Minute).To(gotime.Millisecond))
	ds4h := ds.SliceByTime(initialTime - time.New(4, gotime.Hour).To(gotime.Millisecond))
	ds24h := ds.SliceByTime(initialTime - time.New(24, gotime.Hour).To(gotime.Millisecond))

	now := gotime.Now()
	recursion(ds5min)
	recursion(ds30min)
	recursion(ds4h)
	recursion(ds24h)
	log.Printf("consecutive took %.3fms", float64(gotime.Since(now).Nanoseconds())/float64(gotime.Millisecond))

	now = gotime.Now()

	tupleChan := parallel(
		[]*TupleInput{
			{
				name: "5min",
				fn: func() *data.Stats {
					return recursion(ds5min)
				},
			},
			{
				name: "30min",
				fn: func() *data.Stats {
					return recursion(ds30min)
				},
			},
			{
				name: "4h",
				fn: func() *data.Stats {
					return recursion(ds4h)
				},
			},
			{
				name: "24h",
				fn: func() *data.Stats {
					return recursion(ds24h)
				},
			},
		},
	)

	for tuple := range tupleChan {
		log.Printf("%s %.3fms %+v", tuple.name, tuple.durationMs, tuple.stats)
	}

	log.Printf("parallel took %.3fms", float64(gotime.Since(now).Nanoseconds())/float64(gotime.Millisecond))
}

func recursion(ds data.Set) *data.Stats {
	if len(ds) <= int(math.Pow(2, 13)) {
		return ds.CalcStats()
	}

	var res [2]*data.Stats
	var wg sync.WaitGroup

	wg.Add(2)

	go func() {
		defer wg.Done()
		res[0] = recursion(ds[:len(ds)/2])
	}()
	go func() {
		defer wg.Done()
		res[1] = recursion(ds[len(ds)/2:])
	}()

	wg.Wait()

	return res[0].MergeWith(*res[1])
}

type TupleInput struct {
	name string
	fn   func() *data.Stats
}

type TupleOutput struct {
	name       string
	stats      *data.Stats
	durationMs float32
}

func parallel(tuples []*TupleInput) <-chan *TupleOutput {
	tupleChan := make(chan *TupleOutput, len(tuples))

	var wg sync.WaitGroup
	wg.Add(len(tuples))

	for _, tuple := range tuples {
		go func(tuple *TupleInput) {
			defer wg.Done()
			now := gotime.Now()
			stats := tuple.fn()
			tupleChan <- &TupleOutput{
				name:       tuple.name,
				stats:      stats,
				durationMs: float32(gotime.Since(now).Nanoseconds()) / float32(gotime.Millisecond),
			}
		}(tuple)
	}

	go func() {
		wg.Wait()
		close(tupleChan)
	}()

	return tupleChan
}
