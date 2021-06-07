package data

import "math"

type Point struct {
	Rate      float64 `json:"rate" required:"true" db:"rate"`
	Timestamp uint64  `json:"timestamp" required:"true" db:"timestamp"`
}

type Set []Point

type Stats struct {
	Average float64 `json:"average"`
	High    float64 `json:"high"`
	Low     float64 `json:"low"`
	Open    float64 `json:"open"`
	Close   float64 `json:"close"`
}

func (s Stats) MergeWith(s2 Stats) *Stats {
	return &Stats{
		Open:    s2.Open,
		Close:   s.Close,
		High:    math.Max(s.High, s2.High),
		Low:     math.Min(s.Low, s2.Low),
		Average: (s.Average + s2.Average) / 2.0,
	}
}

func (s Set) SliceByTime(time uint64) Set {
	for i, p := range s {
		if p.Timestamp < time {
			return s[:i]
		}
	}
	return s
}

func (s Set) CalcStats() *Stats {
	res := Stats{
		Low:   math.MaxFloat64,
		Open:  s[len(s)-1].Rate,
		Close: s[0].Rate,
	}
	for _, p := range s {
		res.Low = math.Min(res.Low, p.Rate)
		res.High = math.Max(res.High, p.Rate)
		res.Average += p.Rate
	}
	res.Average /= float64(len(s))

	return &res
}
