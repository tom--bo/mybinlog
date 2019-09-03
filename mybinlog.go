package main

import (
	"encoding/binary"
	"errors"
)

func process(buf []byte) ([]Event, Counter, error) {
	l := len(buf)

	if l < 4 || string(buf[1:4]) != "bin" {
		return nil, Counter{}, errors.New("This is not a binlog")
	}

	// initialize each binlog file
	events := []Event{}
	c := Counter{}
	c.event = make(map[LogEventType]int, 37)

	pos := 4
	for pos+19 < l-1 && pos != 0 {
		if int64(binary.LittleEndian.Uint32(buf[pos:pos+4])) == 0 {
			// remaining bytes
			break
		}
		c.total += 1

		// process header
		head := parseHeader(buf[pos : pos+19])
		c.event[head.Typecode] += 1

		// process body
		b, err := parseData(head.Typecode, buf[pos+19:head.NextPosition])
		if err != nil {
			// fmt.Println(err)
			c.error += 1
			pos = head.NextPosition
			continue
		} else {
			c.success += 1
		}

		if b.GetType() == "UnknownEvent" {
			c.unknown += 1
		}

		event := Event{
			Header: head,
			Body:   b,
		}
		events = append(events, event)

		pos = head.NextPosition
	}

	return events, c, nil
}
