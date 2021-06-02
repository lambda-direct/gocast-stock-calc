package main

import (
	"bufio"
	"encoding/json"
	"log"
	"math"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/lambda-direct/gocast-stock-calc/data"
)

type Stats struct {
	Average float64
	High    float64
	Low     float64
	Open    float64
	Close   float64
}

func main() {
	var ds data.Set

	file, err := os.Open("data.json")
	if err != nil {
		panic(err)
	}

	fileReader := bufio.NewReader(file)

	if err := json.NewDecoder(fileReader).Decode(&ds); err != nil {
		panic(err)
	}

	//defaultTime := time.Unix(1616761300, int64(205*time.Millisecond))
	defaultTime := time.Unix(1616761301, 0)

	ds5min := getDataSetForTimestamp(ds, defaultTime.Add(-5*time.Minute))
	ds30min := getDataSetForTimestamp(ds, defaultTime.Add(-30*time.Minute))
	ds4h := getDataSetForTimestamp(ds, defaultTime.Add(-4*time.Hour))
	ds24h := getDataSetForTimestamp(ds, defaultTime.Add(-24*time.Hour))

	results := make([]Stats, 4)
	var wg sync.WaitGroup
	wg.Add(4)
	go func() {
		defer wg.Done()
		results[0] = *calcStats(ds5min)
	}()
	go func() {
		defer wg.Done()
		results[1] = *calcStats(ds30min)
	}()
	go func() {
		defer wg.Done()
		results[2] = *calcStats(ds4h)
	}()
	go func() {
		defer wg.Done()
		results[3] = *calcStats(ds24h)
	}()

	wg.Wait()

	log.Printf("%+v", results)
}

func getDataSetForTimestamp(ds data.Set, t time.Time) data.Set {
	for i, p := range ds {
		if t.After(time.Time(p.Timestamp)) {
			return ds[:i-1]
		}
	}
	return ds
}

func calcStats(ds data.Set) *Stats {
	if len(ds) <= 8192 {
		return calcStatsSingle(ds)
	}

	statsArr := make([]*Stats, 2)
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		statsArr[0] = calcStats(ds[:len(ds)/2])
	}()
	go func() {
		defer wg.Done()
		statsArr[1] = calcStats(ds[len(ds)/2:])
	}()

	wg.Wait()

	stats := &Stats{
		High:    math.Max(statsArr[0].High, statsArr[1].High),
		Low:     math.Min(statsArr[0].Low, statsArr[1].Low),
		Average: (statsArr[0].Average + statsArr[1].Average) / 2,
		Open:    statsArr[1].Open,
		Close:   statsArr[0].Close,
	}

	return stats
}

func calcStatsSingle(ds data.Set) *Stats {
	s := Stats{
		Low: math.MaxInt64,
	}
	for _, p := range ds {
		s.High = math.Max(s.High, p.Rate)
		s.Low = math.Min(s.Low, p.Rate)
		s.Average += p.Rate
	}

	if len(ds) > 0 {
		s.Average /= float64(len(ds))
		s.Open = ds[len(ds)-1].Rate
		s.Close = ds[0].Rate
	}

	return &s
}

func getMemUsage() (uint64, uint64) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return bToMb(m.Alloc), bToMb(m.TotalAlloc)
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
