package main

import (
	"encoding/binary"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"sync"
	gotime "time"

	"github.com/lambda-direct/gocast-stock-calc/data"
	"github.com/lambda-direct/gocast-stock-calc/time"
	"github.com/lambda-direct/gocast-stock-calc/util"
)

func main() {
	allStart := gotime.Now()

	f, err := os.Open("data.bin")
	util.PanicOnError(err)

	ds, err := readDataSet(f)
	util.PanicOnError(err)

	log.Printf("parse took %dms", gotime.Since(allStart).Milliseconds())

	initialTime := ds[0].Timestamp

	start := gotime.Now()

	ds5min := ds.SliceByTime(initialTime - time.New(5, gotime.Minute).To(gotime.Millisecond))
	ds30min := ds.SliceByTime(initialTime - time.New(30, gotime.Minute).To(gotime.Millisecond))
	ds4h := ds.SliceByTime(initialTime - time.New(4, gotime.Hour).To(gotime.Millisecond))
	ds24h := ds.SliceByTime(initialTime - time.New(24, gotime.Hour).To(gotime.Millisecond))

	//now := gotime.Now()
	//recursion(ds5min)
	//recursion(ds30min)
	//recursion(ds4h)
	//recursion(ds24h)
	//log.Printf("consecutive took %.3fms", float64(gotime.Since(now).Nanoseconds())/float64(gotime.Millisecond))

	//var wg sync.WaitGroup
	//wg.Add(4)
	//
	//go func() *data.Stats {
	//	defer wg.Done()
	//	return recursion(ds5min)
	//}()
	//
	//go func() *data.Stats {
	//	defer wg.Done()
	//	return recursion(ds30min)
	//}()
	//
	//go func() *data.Stats {
	//	defer wg.Done()
	//	return recursion(ds4h)
	//}()
	//
	//go func() *data.Stats {
	//	defer wg.Done()
	//	return recursion(ds24h)
	//}()
	//
	//wg.Wait()

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

	log.Printf("parallel took %.3fms", float64(gotime.Since(start).Nanoseconds())/float64(gotime.Millisecond))

	start = gotime.Now()

	res := dynamic(ds, []uint64{
		initialTime - time.New(5, gotime.Minute).To(gotime.Millisecond),
		initialTime - time.New(30, gotime.Minute).To(gotime.Millisecond),
		initialTime - time.New(4, gotime.Hour).To(gotime.Millisecond),
		initialTime - time.New(24, gotime.Hour).To(gotime.Millisecond),
	}, initialTime)

	log.Printf("dynamic took %dms", gotime.Since(start).Milliseconds())

	log.Println(gotime.Unix(int64(initialTime/1000), int64(gotime.Duration(initialTime%1000)*gotime.Millisecond)).Format(gotime.RFC1123))
	for ts, stats := range res {
		log.Printf("ts=%s stats=%#v", gotime.Unix(int64(ts/1000), int64(gotime.Duration(ts%1000)*gotime.Millisecond)).Format(gotime.RFC1123), stats)
	}

	log.Printf("all took %dms", gotime.Since(allStart).Milliseconds())
}

func recursion(ds data.Set) *data.Stats {
	if len(ds) <= 4000 {
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

func dynamic(ds data.Set, timestamps []uint64, initialTimestamp uint64) map[uint64]*data.Stats {
	res := make(map[uint64]*data.Stats, len(timestamps))

	dsChunks := make([]data.Set, len(timestamps))

	prevTimestamp := initialTimestamp
	for i, ts := range timestamps {
		from := (initialTimestamp - prevTimestamp) / 10
		to := (initialTimestamp-ts)/10 + 1
		if to > uint64(len(ds)) {
			to = uint64(len(ds))
		}
		dsChunks[i] = ds[from:to]

		prevTimestamp = ts
	}

	for i, chunk := range dsChunks {
		s := calcStats(chunk)
		if i > 0 {
			s = res[timestamps[i-1]].MergeWith(*s)
		}
		res[timestamps[i]] = s
	}

	return res
}

func calcStats(ds data.Set) *data.Stats {
	const DataChunkSize = 2000
	ChunkAmount := int(math.Ceil(float64(len(ds)) / DataChunkSize))

	statsList := make([]*data.Stats, ChunkAmount)

	var wg sync.WaitGroup
	wg.Add(ChunkAmount)

	for i := 0; i < ChunkAmount; i++ {
		go func(i int) {
			defer wg.Done()
			from := i * DataChunkSize
			to := int(math.Min(float64((i+1)*DataChunkSize), float64(len(ds))))
			statsList[i] = ds[from:to].CalcStats()
		}(i)
	}

	wg.Wait()

	res := statsList[0]
	for _, s := range statsList[1:] {
		res = res.MergeWith(*s)
	}

	return res
}

func parsePoint(b []byte) data.Point {
	ts := binary.LittleEndian.Uint64(b[:8])
	rateUint64 := binary.LittleEndian.Uint64(b[8:16])
	rate := math.Float64frombits(rateUint64)
	return data.Point{
		Timestamp: ts,
		Rate:      rate,
	}
}

func readDataSet(f io.Reader) (data.Set, error) {
	const GoroutinesAmount = 100
	const DataSetSize = 8_640_000
	const DataChunkSize = DataSetSize / GoroutinesAmount

	bytes, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}

	ds := make(data.Set, DataSetSize)

	var wg sync.WaitGroup

	for i := 0; i < GoroutinesAmount; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			for j := 0; j < DataChunkSize; j++ {
				offset := i*DataChunkSize + j*16
				ds[i*DataChunkSize+j] = parsePoint(bytes[offset : offset+16])
			}
		}(i)
	}

	wg.Wait()

	return ds, nil
}
