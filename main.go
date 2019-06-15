package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

func parseData(typeCode LogEventType, d []byte) Ibody {
	switch typeCode {
	case UNKNOWN_EVENT:
		return UnknownEvent{}
	case START_EVENT_V3:
	case QUERY_EVENT:
	case STOP_EVENT:
	case ROTATE_EVENT:
	case INTVAR_EVENT:
	case LOAD_EVENT:
	case SLAVE_EVENT:
	case CREATE_FILE_EVENT:
	case APPEND_BLOCK_EVENT:
	case EXEC_LOAD_EVENT:
	case DELETE_FILE_EVENT:
	case NEW_LOAD_EVENT:
	case RAND_EVENT:
	case USER_VAR_EVENT:
	case FORMAT_DESCRIPTION_EVENT: // 15
		ret := FormatDescriptionEvent{
			BinlogEvent: int(binary.LittleEndian.Uint16(d[:2])),
			ServerVersion: string(d[2:52]),
			CreateTimeStamp: time.Unix(int64(binary.LittleEndian.Uint32(d[52:56])), 0),
			HeaderLength: int(d[56]),
			PostHeaderLength: d[57:],
		}
		return ret
	case XID_EVENT:
	case BEGIN_LOAD_QUERY_EVENT:
	case EXECUTE_LOAD_QUERY_EVENT:
	case TABLE_MAP_EVENT:
	case PRE_GA_WRITE_ROWS_EVENT:
	case PRE_GA_UPDATE_ROWS_EVENT:
	case PRE_GA_DELETE_ROWS_EVENT:
	case WRITE_ROWS_EVENT:
	case UPDATE_ROWS_EVENT:
	case DELETE_ROWS_EVENT:
	case INCIDENT_EVENT:
	case HEARTBEAT_LOG_EVENT:
	case IGNORABLE_LOG_EVENT:
	case ROWS_QUERY_LOG_EVENT:
	case WRITE_ROWS_EVENT2:
	case UPDATE_ROWS_EVENT2:
	case DELETE_ROWS_EVENT2:
	case GTID_LOG_EVENT:
	case ANONYMOUS_GTID_LOG_EVENT:
	case PREVIOUS_GTIDS_LOG_EVENT:
	case ENUM_END_EVENT:
	}
	return UnknownEvent{}
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
	}

	l := len(buf)

	if l < 4 || string(buf[1:4]) != "bin" {
		fmt.Println("This is not a binlog!!")
		return
	}

	events := []Event{}
	pos := 4
	for pos+19 < l-1 && pos != 0 {
		if int64(binary.LittleEndian.Uint32(buf[pos : pos+4])) == 0 {
			// remaining bytes
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

		// fmt.Println(head)
		b := parseData(head.Typecode, buf[pos+19:head.NextPosition])

		if b.GetType() != "UnknownEvent" {
			fmt.Println(head)
			fmt.Println("----")
			fmt.Println(b)
			fmt.Println("----")
		}

		event := Event {
			header: head,
			body: b,
		}
		events = append(events, event)

		pos = head.NextPosition
	}
}
