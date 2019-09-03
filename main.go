package main

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"
)

var doPrint, doPrintJSON bool

func readFile(filename string) ([]byte, error) {
	f, err := os.Open(filename)
	if err != nil {
		fmt.Println("Can't open a file")
	}
	defer f.Close()
	fInfo, err := f.Stat()
	if err != nil {
		return nil, err
	}
	buf := make([]byte, fInfo.Size())

	for {
		n, err := f.Read(buf)
		if n == 0 {
			break
		}
		if err != nil {
			return nil, err
		}
		break
	}
	return buf, nil
}

type Counter struct {
	total   int
	success int
	unknown int
	error   int
	event   map[LogEventType]int
}

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

		ts := int64(binary.LittleEndian.Uint32(buf[pos : pos+4]))
		head := Header{
			Timestamp:    time.Unix(ts, 0),
			Typecode:     LogEventType(int(buf[pos+4])),
			ServerID:     int(binary.LittleEndian.Uint32(buf[pos+5 : pos+9])),
			Eventlength:  int(binary.LittleEndian.Uint32(buf[pos+9 : pos+13])),
			NextPosition: int(binary.LittleEndian.Uint32(buf[pos+13 : pos+17])),
			Flags:        int(binary.LittleEndian.Uint16(buf[pos+17 : pos+19])),
		}

		c.event[head.Typecode] += 1
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

func printCount(c Counter) {
	fmt.Println("totalCount: ", c.total)
	fmt.Println("successCount: ", c.success)
	fmt.Println("errorCount: ", c.error)
	fmt.Println("unknownCount: ", c.unknown)

	fmt.Println("-- Event count -- ")
	for k, v := range c.event {
		fmt.Printf("%25s: %d\n", k, v)
	}
}

func printEvents(events []Event) {
	for _, e := range events {
		if e.Header.Typecode != UNKNOWN_EVENT {
			if doPrint {
				fmt.Println(e.Header)
				fmt.Printf("%+v", e.Body)
				fmt.Println("\n----------------------------\n")
			}
		}

		if doPrintJSON {
			j, err := json.Marshal(e)
			if err != nil {
				fmt.Println(err)
				break
			}
			fmt.Println(string(j))
		}

	}

}

func main() {
	// flags
	flag.BoolVar(&doPrint, "p", false, "Do print events")
	flag.BoolVar(&doPrintJSON, "j", false, "Do print event headers as JSON")
	flag.Parse()

	// process each files
	for _, filename := range flag.Args() {
		fmt.Println("Read [", filename, "] ...")

		// read files
		buf, err := readFile(filename)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		events, counter, err := process(buf)
		if err != nil {
			fmt.Println(err)
		}
		printCount(counter)
		printEvents(events)
	}
}
