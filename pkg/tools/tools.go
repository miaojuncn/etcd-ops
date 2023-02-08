package tools

import "time"

const (
	timeTemplate = "20060102"
)

func UnixTime2String(t time.Time) string {
	return t.Format(timeTemplate)
}
