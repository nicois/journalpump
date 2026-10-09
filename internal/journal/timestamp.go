package journal

import (
	"fmt"
	"math"
	"time"
)

// TimestampFromRealtime ports the timestamp derivation in
// _get_or_generate_json (journalpump.py:762-767):
// datetime.fromtimestamp(REALTIME_TIMESTAMP, UTC).replace(tzinfo=None)
// over the float seconds produced by convert_realtime. CPython rounds the
// microsecond value half-to-even (_PyTime ROUND_HALF_EVEN), which
// math.RoundToEven reproduces for the float64 seconds*1e6 product.
func TimestampFromRealtime(seconds float64) (time.Time, error) {
	micros := math.RoundToEven(seconds * 1e6)
	if micros > math.MaxInt64 {
		return time.Time{}, fmt.Errorf("year is out of range for timestamp %v", seconds)
	}
	t := time.UnixMicro(int64(micros)).UTC()
	if year := t.Year(); year < 1 || year > 9999 {
		// Python raises ValueError for datetimes outside MINYEAR..MAXYEAR.
		return time.Time{}, fmt.Errorf("year %d is out of range", year)
	}
	return t, nil
}
