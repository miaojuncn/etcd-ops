package tools

import "time"

const (
	timeTemplate = "20060102"
)

func Unix2String(t time.Time) string {
	return t.Format(timeTemplate)
}
