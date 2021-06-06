package time

import "time"

type Time time.Duration

func (t Time) Ms() uint64 {
	return uint64(time.Duration(t) / time.Millisecond)
}

func Seconds(amount uint64) Time {
	return Time(time.Duration(amount) * time.Second)
}

func Minutes(amount uint64) Time {
	return Time(time.Duration(amount) * time.Minute)
}

func Hours(amount uint64) Time {
	return Time(time.Duration(amount) * time.Hour)
}
