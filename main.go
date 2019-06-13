package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

func getTimestamp(b []byte) (int, error) {
	ts := 0
	return ts, nil
}

func main() {
	doPrintJSON := false
	// headers := []Header{}

	// read files
	f, err := os.Open("binlog.000004")
	if err != nil {
		fmt.Println("Can't open a file")
	}
	defer f.Close()
	buf := make([]byte, 10240)

	for {
		n, err := f.Read(buf)
		if n == 0 {
			break
		}
		if err != nil {
			fmt.Println("Some error happen during reading files")
		}

		fmt.Println(string(buf[:4]))
	}

	pos := 4
	l := len(buf)
	for pos+19 < l-1 && pos != 0 {
		// ts, err := getTimestamp(buf[pos:pos+4])
		if err != nil {
			fmt.Println(err)
			break
		}
		ts := int64(binary.LittleEndian.Uint32(buf[pos : pos+4]))
		head := Header{
			Timestamp:    time.Unix(ts, 0),
			Typecode:     LogEventType(int(buf[pos+4])),
			ServerID:     int(binary.LittleEndian.Uint32(buf[pos+5 : pos+9])),
			Eventlength:  int(binary.LittleEndian.Uint32(buf[pos+9 : pos+13])),
			NextPosition: int(binary.LittleEndian.Uint32(buf[pos+13 : pos+17])),
			Flags:        buf[pos+17 : pos+19],
		}

		if doPrintJSON {
			jsonb, err := json.Marshal(head)
			if err != nil {
				fmt.Println(err)
				break
			}
			fmt.Println(string(jsonb))
		}

		fmt.Println(head)

		pos = head.NextPosition

	}
}
