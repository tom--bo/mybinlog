package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"time"
)

func parseHeader(d []byte) Header {
	pos := 0
	ts := int64(binary.LittleEndian.Uint32(d[pos : pos+4]))
	h := Header{
		Timestamp:    time.Unix(ts, 0),
		Typecode:     LogEventType(int(d[pos+4])),
		ServerID:     int(binary.LittleEndian.Uint32(d[pos+5 : pos+9])),
		Eventlength:  int(binary.LittleEndian.Uint32(d[pos+9 : pos+13])),
		NextPosition: int(binary.LittleEndian.Uint32(d[pos+13 : pos+17])),
		Flags:        int(binary.LittleEndian.Uint16(d[pos+17 : pos+19])),
	}

	return h
}

func parseStatusVariables(d []byte) StatusVariable {
	return StatusVariable{} // ?? todo
}

func searchNullPosition(d []byte) int {
	for i, b := range d {
		if int(b) == 0 {
			return i
		}
	}
	return -1
}

// parse database_name and SQL_statement
func parseAfterStatusVariables(d []byte, dbnamelen int) (string, string) {
	return string(d[:dbnamelen]), string(d[dbnamelen+1 : len(d)-4]) // ?? there is unknown 4 byte after sql_statement
}

func getEnumFieldType(d []byte) []EnumFieldTypes {
	ret := []EnumFieldTypes{}
	for _, b := range d {
		ret = append(ret, EnumFieldTypes(int(b)))
	}
	return ret
}

func getBitField(d []byte) string {
	ret := ""
	for _, b := range d {
		s := fmt.Sprintf("%8b", int(b))
		s = strings.Replace(s, " ", "0", -1)
		ret = s + ret
	}

	return ret
}

func parseData(typeCode LogEventType, d []byte) (Ibody, error) {
	switch typeCode {
	case UNKNOWN_EVENT:
		return UnknownEvent{}, nil
	case START_EVENT_V3:
		return StartEventV3{}, nil
	case QUERY_EVENT:
		dbnamelen := int(d[8])
		statusVarLen := int(binary.LittleEndian.Uint16(d[11:13]))
		statusVariable := StatusVariable{}
		if statusVarLen != 0 {
			statusVariable = parseStatusVariables(d[13 : 13+statusVarLen])
		}
		dbname, sql := parseAfterStatusVariables(d[13+statusVarLen:], dbnamelen)

		ret := QueryEvent{
			ThreadID:        int(binary.LittleEndian.Uint16(d[:4])),
			ExecutionTime:   int(binary.LittleEndian.Uint16(d[4:8])),
			DBNameLen:       dbnamelen,
			ErrorCode:       int(binary.LittleEndian.Uint16(d[9:11])),
			StatusVarLen:    statusVarLen,
			StatusVariables: statusVariable,
			DatabaseName:    dbname,
			SQLStatement:    sql,
		}
		return ret, nil
	case STOP_EVENT:
		return StopEvent{}, nil
	case ROTATE_EVENT:
		ret := RotateEvent{
			NextPos:  int(binary.LittleEndian.Uint16(d[:8])),
			NextName: string(d[8 : len(d)-4]),
		}
		return ret, nil
	case INTVAR_EVENT:
		if len(d) != 9+4 {
			return IntVar{}, errors.New("Unexpected data in INTVAR_EVENT")
		}
		ret := IntVar{
			Opt1:  IntVarOpt(d[0]),
			Value: int(binary.LittleEndian.Uint64(d[1 : len(d)-4])),
		}
		return ret, nil
	case LOAD_EVENT:
		return LoadEvent{}, nil
	case SLAVE_EVENT:
		return UnknownEvent{}, errors.New("SLAVE_EVENT is never used...")
	case CREATE_FILE_EVENT:
		return CreateFileEvent{}, nil
	case APPEND_BLOCK_EVENT:
		ret := AppendBlock{
			ID:   int(binary.LittleEndian.Uint32(d[:4])),
			Data: d[4:],
		}
		return ret, nil
	case EXEC_LOAD_EVENT:
		ret := ExecLoad{
			ID: int(binary.LittleEndian.Uint32(d[:4])),
		}
		return ret, nil
	case DELETE_FILE_EVENT:
		ret := DeleteFile{
			ID: int(binary.LittleEndian.Uint32(d[:4])),
		}
		return ret, nil
	case NEW_LOAD_EVENT:
		return NewLoadEvent{}, nil
	case RAND_EVENT:
		if len(d) != 20 {
			return Rand{}, errors.New("Unexpected data in RandEvent")
		}
		return Rand{
			FirstSeed:  int(binary.LittleEndian.Uint64(d[:8])),
			SecondSeed: int(binary.LittleEndian.Uint64(d[8:16])),
		}, nil

	case USER_VAR_EVENT:
		return UserVarEvent{}, nil
	case FORMAT_DESCRIPTION_EVENT: // 15
		nullpos := searchNullPosition(d[2:52])
		ret := FormatDescriptionEvent{
			BinlogEvent:      int(binary.LittleEndian.Uint16(d[:2])),
			ServerVersion:    string(d[2 : 2+nullpos]),
			CreateTimeStamp:  time.Unix(int64(binary.LittleEndian.Uint32(d[52:56])), 0),
			HeaderLength:     int(d[56]),
			PostHeaderLength: d[57:],
		}
		return ret, nil
	case XID_EVENT:
		if len(d) != 12 {
			return XID{}, errors.New("Unexpected data in XIDEvent")
		}
		return XID{
			XID: int(binary.LittleEndian.Uint64(d[:8])),
		}, nil
	case BEGIN_LOAD_QUERY_EVENT:
		return BeginLoadQuery{
			ID:   int(binary.LittleEndian.Uint32(d[:4])),
			Data: d[4 : len(d)-4],
		}, nil
	case EXECUTE_LOAD_QUERY_EVENT:
		return ExecuteLoadQueryEvent{}, nil
	case TABLE_MAP_EVENT:
		dbNameLen := int(d[8])
		tableNameLen := int(d[8+dbNameLen+2]) // DBName is terminated by NULL
		tableNamePos := 9 + dbNameLen + 2
		numOfColPos := tableNamePos + tableNameLen + 1
		numOfCol := int(d[numOfColPos])
		metaBlockLenPos := numOfColPos + 1 + numOfCol
		isNullPos := metaBlockLenPos + 1 + int(d[metaBlockLenPos])
		return TableMapEvent{
			TableID:      int(binary.LittleEndian.Uint64(append(d[:6], []byte{0, 0}...))),
			ReservedByte: d[6:8],
			DBNameLen:    dbNameLen,
			DBName:       string(d[9 : 9+dbNameLen]),
			TableNameLen: tableNameLen,
			TableName:    string(d[tableNamePos : tableNamePos+tableNameLen]),
			NumOfCol:     int(d[numOfColPos]),
			ColType:      getEnumFieldType(d[numOfColPos+1 : numOfColPos+1+numOfCol]),
			MetaBlockLen: int(d[metaBlockLenPos]),
			MetaBlock:    d[metaBlockLenPos+1 : isNullPos],
			NullColumns:  getBitField(d[isNullPos : isNullPos+(numOfCol+7)/8]),
		}, nil
	case PRE_GA_WRITE_ROWS_EVENT:
		return PreGAWriteRows{}, nil
	case PRE_GA_UPDATE_ROWS_EVENT:
		return PreGAUpdateRows{}, nil
	case PRE_GA_DELETE_ROWS_EVENT:
		return PreGADeleteRows{}, nil
	case WRITE_ROWS_EVENT: // not support
		return WriteRows{}, nil
	case UPDATE_ROWS_EVENT: // not support
		return UpdateRows{}, nil
	case DELETE_ROWS_EVENT: // not support
		return DeleteRows{}, nil
	case INCIDENT_EVENT:
		incidentLen := int(d[1])
		m := ""
		if incidentLen != 0 {
			m = string(d[2 : 2+incidentLen])
		}
		return Incident{
			IncidentNum: int(d[0]),
			MessageLen:  incidentLen,
			Message:     m,
		}, nil
	case HEARTBEAT_LOG_EVENT:
		return HeartbeatLog{}, nil
	case IGNORABLE_LOG_EVENT:
		return IgnorableLogEvent{}, nil
	case ROWS_QUERY_LOG_EVENT:
		return RowsQueryLogEvent{}, nil
	case WRITE_ROWS_EVENT2:
		numOfColPos := 10
		numOfCol := int(d[numOfColPos])
		isUsedEndPos := numOfColPos + 1 + (numOfCol+7)/8
		isNullEndPos := isUsedEndPos + (numOfCol+7)/8
		return WriteRows{
			TableID:       int(binary.LittleEndian.Uint64(append(d[:6], []byte{0, 0}...))),
			ReservedByte:  d[6:10],
			NumOfCol:      numOfCol,
			IsUsedAfter:   getBitField(d[numOfColPos+1 : isUsedEndPos]),
			IsNullAfter:   getBitField(d[isUsedEndPos:isNullEndPos]),
			AfterImage:    d[isNullEndPos : len(d)-4],
			AfterNumOfCol: d[numOfColPos+1:],
		}, nil
	case UPDATE_ROWS_EVENT2:
		numOfColPos := 10
		numOfCol := int(d[numOfColPos])
		isUsedBeforeEndPos := numOfColPos + 1 + (numOfCol+7)/8
		isUsedAfterEndPos := isUsedBeforeEndPos + (numOfCol+7)/8
		isNullBeforeEndPos := isUsedAfterEndPos + (numOfCol+7)/8
		isNullAfterEndPos := isNullBeforeEndPos + (numOfCol+7)/8
		return UpdateRows{
			TableID:             int(binary.LittleEndian.Uint64(append(d[:6], []byte{0, 0}...))),
			ReservedByte:        d[6:10],
			NumOfCol:            numOfCol,
			IsUsedBefore:        getBitField(d[numOfColPos+1 : isUsedBeforeEndPos]),
			IsUsedAfter:         getBitField(d[isUsedBeforeEndPos:isUsedAfterEndPos]),
			IsNullBefore:        getBitField(d[isUsedAfterEndPos:isNullBeforeEndPos]),
			IsNullAfter:         getBitField(d[isNullBeforeEndPos:isNullAfterEndPos]),
			BeforeAndAfterImage: d[isNullAfterEndPos : len(d)-4], // todo
			AfterNumOfCol:       d[numOfColPos+1:],
		}, nil
	case DELETE_ROWS_EVENT2:
		numOfColPos := 10
		numOfCol := int(d[numOfColPos])
		isUsedEndPos := numOfColPos + 1 + (numOfCol+7)/8
		isNullEndPos := isUsedEndPos + (numOfCol+7)/8
		return DeleteRows{
			TableID:      int(binary.LittleEndian.Uint64(append(d[:6], []byte{0, 0}...))),
			ReservedByte: d[6:10],
			NumOfCol:     numOfCol,
			IsUsed:       getBitField(d[numOfColPos+1 : isUsedEndPos]),
			IsNull:       getBitField(d[isUsedEndPos:isNullEndPos]),
			AfterImage:   d[isNullEndPos : len(d)-4],
		}, nil
	case GTID_LOG_EVENT:
		return GtidLogEvent{}, nil
	case ANONYMOUS_GTID_LOG_EVENT:
		return AnonymousGtidLogEvent{}, nil
	case PREVIOUS_GTIDS_LOG_EVENT:
		return PreviousGtidsLogEvent{}, nil
	case ENUM_END_EVENT:
		return EnumEndEvent{}, nil
	}

	return UnknownEvent{}, errors.New("Can't detect event")
}
