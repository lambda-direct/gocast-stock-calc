package time

import "time"

type Time time.Duration

func New(amount int64, unit time.Duration) Time {
	return Time(amount) * Time(unit)
}

func (t Time) To(unit time.Duration) uint64 {
	return uint64(time.Duration(t) / unit)
}
