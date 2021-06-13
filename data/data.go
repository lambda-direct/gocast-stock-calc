package data

import "math"

type Point struct {
	Rate      float64 `json:"rate" required:"true" db:"rate"`
	Timestamp uint64  `json:"timestamp" required:"true" db:"timestamp"`
}

type Set []Point

type Stats struct {
	Average float64 `json:"average"`
	Sum     float64
	High    float64 `json:"high"`
	Low     float64 `json:"low"`
	Open    float64 `json:"open"`
	Close   float64 `json:"close"`
	Count   int
}

func (s Stats) MergeWith(s2 Stats) *Stats {
	count := s.Count + s2.Count
	sum := s.Sum + s2.Sum
	return &Stats{
		Open:    s2.Open,
		Close:   s.Close,
		High:    math.Max(s.High, s2.High),
		Low:     math.Min(s.Low, s2.Low),
		Average: sum / float64(count),
		Sum:     sum,
		Count:   count,
	}
}

func (s Set) SliceByTime(time uint64) Set {
	initialTimestamp := s[0].Timestamp
	from := 0
	to := (initialTimestamp-time)/10 + 1
	if to > uint64(len(s)) {
		to = uint64(len(s))
	}
	return s[from:to]
}

func (s Set) CalcStats() *Stats {
	res := Stats{
		Low:   math.MaxFloat64,
		Open:  s[len(s)-1].Rate,
		Close: s[0].Rate,
		Count: len(s),
	}
	for _, p := range s {
		res.Low = math.Min(res.Low, p.Rate)
		res.High = math.Max(res.High, p.Rate)
		res.Sum += p.Rate
	}

	return &res
}
