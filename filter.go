package main

import (
	"time"
)

type Filter struct {
	Events    map[Event]bool // 1: use, 0: ignore
	StartTime time.Time
	EndTime   time.Time
	StartPos  time.Time
	EndPos    time.Time
	Database  map[string]bool
	Table     map[string]bool
}

func (f Filter) InitFilter() {

}
