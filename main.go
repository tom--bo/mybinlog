package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

var eventCount map[LogEventType]int

func parseStatusVariables(d []byte) StatusVariable {
	return StatusVariable{} // ?? todo
}

/*
func searchNullPosition(d []byte) int {
	for i, b := range d {
		if b == 0 {
			return i
		}
	}
	return -1
}
*/

// parse database_name and SQL_statement
func parseAfterStatusVariables(d []byte, dbnamelen int) (string, string) {
	return string(d[:dbnamelen]), string(d[dbnamelen+1:len(d)-4]) // ?? there is unknown 4 byte after sql_statement
}

func parseData(typeCode LogEventType, d []byte) Ibody {
	eventCount[typeCode] += 1
	switch typeCode {
	case UNKNOWN_EVENT:
		return UnknownEvent{}
	case START_EVENT_V3:
	case QUERY_EVENT:
		dbnamelen := int(d[8])
		statusVarLen := int(binary.LittleEndian.Uint16(d[11:13]))
		statusVariable := StatusVariable{}
		if statusVarLen != 0 {
			statusVariable = parseStatusVariables(d[13:13+statusVarLen])
		}
		dbname, sql := parseAfterStatusVariables(d[13+statusVarLen:], dbnamelen)

		ret := QueryEvent{
			ThreadID: int(binary.LittleEndian.Uint16(d[:4])),
			ExecutionTime: int(binary.LittleEndian.Uint16(d[4:8])),
			DBNameLen: dbnamelen,
			ErrorCode: int(binary.LittleEndian.Uint16(d[9:11])),
			StatusVarLen: statusVarLen,
			StatusVariables: statusVariable,
			DatabaseName: dbname,
			SQLStatement: sql,
		}
		return ret
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
	eventCount = make(map[LogEventType]int, 37)

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
	totalCount := 0
	unknownCount := 0
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
			fmt.Println(b.GetType())
			fmt.Println(b)
			fmt.Println("----\n")
		} else {
			unknownCount += 1
		}
		totalCount += 1

		event := Event {
			header: head,
			body: b,
		}
		events = append(events, event)

		pos = head.NextPosition
	}

	fmt.Println("totalCount: ", totalCount)
	fmt.Println("unknownCount: ", unknownCount)
	fmt.Println("-- Event count -- ")
	for k,v := range eventCount {
		fmt.Printf("%25s: %d\n", k, v)
	}
}
