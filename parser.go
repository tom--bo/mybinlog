package main

import (
	"encoding/binary"
	"errors"
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
	return string(d[:dbnamelen]), string(d[dbnamelen+1 : len(d)-4]) // ?? there is unknown 4 byte after sql_statement
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
			OptVal1: int(d[0]),
			OptVal2: int(binary.LittleEndian.Uint64(d[1 : len(d)-4])),
		}
		return ret, nil
	case LOAD_EVENT:
	case SLAVE_EVENT:
		return UnknownEvent{}, errors.New("SLAVE_EVENT is never used...")
	case CREATE_FILE_EVENT:
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
	case RAND_EVENT:
		if len(d) != 20 {
			return Rand{}, errors.New("Unexpected data in RandEvent")
		}
		return Rand{
			FirstSeed:  int(binary.LittleEndian.Uint64(d[:8])),
			SecondSeed: int(binary.LittleEndian.Uint64(d[8:16])),
		}, nil

	case USER_VAR_EVENT:
	case FORMAT_DESCRIPTION_EVENT: // 15
		ret := FormatDescriptionEvent{
			BinlogEvent:      int(binary.LittleEndian.Uint16(d[:2])),
			ServerVersion:    string(d[2:52]),
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
			ColType:      d[numOfColPos+1 : numOfColPos+1+numOfCol],
			MetaBlockLen: int(d[metaBlockLenPos]),
			MetaBlock:    d[metaBlockLenPos+1 : isNullPos],
			IsNull:       d[isNullPos : isNullPos+(numOfCol+7)/8],
		}, nil
	case PRE_GA_WRITE_ROWS_EVENT:
		return PreGAWriteRows{}, nil
	case PRE_GA_UPDATE_ROWS_EVENT:
		return PreGAUpdateRows{}, nil
	case PRE_GA_DELETE_ROWS_EVENT:
		return PreGADeleteRows{}, nil
	case WRITE_ROWS_EVENT: // not support
	case UPDATE_ROWS_EVENT: // not support
	case DELETE_ROWS_EVENT: // not support
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
	case ROWS_QUERY_LOG_EVENT:
	case WRITE_ROWS_EVENT2:
		numOfColPos := 8
		numOfCol := int(d[numOfColPos])
		isUsedEndPos := numOfColPos + 1 + (numOfCol+7)/8
		isNullEndPos := isUsedEndPos + (numOfCol+7)/8
		return WriteRows{
			TableID:      int(binary.LittleEndian.Uint64(append(d[:6], []byte{0, 0}...))),
			ReservedByte: d[6:8],
			NumOfCol:     numOfCol,
			IsUsed:       d[numOfColPos+1 : isUsedEndPos],
			IsNull:       d[isUsedEndPos:isNullEndPos],
			AfterImage:   d[isNullEndPos : len(d)-4],
		}, nil
	case UPDATE_ROWS_EVENT2:
		numOfColPos := 8
		numOfCol := int(d[numOfColPos])
		isUsedBeforeEndPos := numOfColPos + 1 + (numOfCol+7)/8
		isUsedAfterEndPos := isUsedBeforeEndPos + (numOfCol+7)/8
		isNullEndPos := isUsedAfterEndPos + (numOfCol+7)/8
		return UpdateRows{
			TableID:      int(binary.LittleEndian.Uint64(append(d[:6], []byte{0, 0}...))),
			ReservedByte: d[6:8],
			NumOfCol:     numOfCol,
			IsUsedBefore: d[numOfColPos+1 : isUsedBeforeEndPos],
			IsUsedAfter:  d[isUsedBeforeEndPos:isUsedAfterEndPos],
			IsNull:       d[isUsedAfterEndPos:isNullEndPos],
			BeforeImage:  d[isNullEndPos : len(d)-4], // todo
			AfterImage:   d[isNullEndPos : len(d)-4], // todo
		}, nil
	case DELETE_ROWS_EVENT2:
		numOfColPos := 8
		numOfCol := int(d[numOfColPos])
		isUsedEndPos := numOfColPos + 1 + (numOfCol+7)/8
		isNullEndPos := isUsedEndPos + (numOfCol+7)/8
		return DeleteRows{
			TableID:      int(binary.LittleEndian.Uint64(append(d[:6], []byte{0, 0}...))),
			ReservedByte: d[6:8],
			NumOfCol:     numOfCol,
			IsUsed:       d[numOfColPos+1 : isUsedEndPos],
			IsNull:       d[isUsedEndPos:isNullEndPos],
			AfterImage:   d[isNullEndPos : len(d)-4],
		}, nil
	case GTID_LOG_EVENT:
	case ANONYMOUS_GTID_LOG_EVENT:
	case PREVIOUS_GTIDS_LOG_EVENT:
	case ENUM_END_EVENT:
	}
	return UnknownEvent{}, errors.New("Can't detect event")
}
