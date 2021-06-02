package data

import (
	"encoding/json"
	"time"
)

type Timestamp time.Time

func (ts *Timestamp) UnmarshalJSON(value []byte) error {
	var intVal int64
	if err := json.Unmarshal(value, &intVal); err != nil {
		return err
	}

	*ts = Timestamp(time.Unix(intVal/1_000, (intVal%1_000)*1_000_000))

	return nil
}

type Point struct {
	Rate      float64   `json:"rate" required:"true"`
	Timestamp Timestamp `json:"timestamp" required:"true"`
}

type Set = []Point
