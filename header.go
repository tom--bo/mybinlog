package main

import (
	"fmt"
	"time"
)

const layout = "2006-01-02 15:04:05"

type LogEventType int

const (
	UNKNOWN_EVENT LogEventType = iota
	START_EVENT_V3
	QUERY_EVENT
	STOP_EVENT
	ROTATE_EVENT
	INTVAR_EVENT
	LOAD_EVENT
	SLAVE_EVENT
	CREATE_FILE_EVENT
	APPEND_BLOCK_EVENT
	EXEC_LOAD_EVENT
	DELETE_FILE_EVENT
	NEW_LOAD_EVENT
	RAND_EVENT
	USER_VAR_EVENT
	FORMAT_DESCRIPTION_EVENT
	XID_EVENT
	BEGIN_LOAD_QUERY_EVENT
	EXECUTE_LOAD_QUERY_EVENT
	TABLE_MAP_EVENT
	PRE_GA_WRITE_ROWS_EVENT
	PRE_GA_UPDATE_ROWS_EVENT
	PRE_GA_DELETE_ROWS_EVENT
	WRITE_ROWS_EVENT
	UPDATE_ROWS_EVENT
	DELETE_ROWS_EVENT
	INCIDENT_EVENT
	HEARTBEAT_LOG_EVENT
	IGNORABLE_LOG_EVENT
	ROWS_QUERY_LOG_EVENT
	WRITE_ROWS_EVENT2
	UPDATE_ROWS_EVENT2
	DELETE_ROWS_EVENT2
	GTID_LOG_EVENT
	ANONYMOUS_GTID_LOG_EVENT
	PREVIOUS_GTIDS_LOG_EVENT
	ENUM_END_EVENT
)

type Event struct {
	header Header
	body Ibody // event_data
}

type Header struct {
	Timestamp    time.Time
	Typecode     LogEventType
	ServerID     int
	Eventlength  int
	NextPosition int
	Flags        []byte
	ExtraHeader  []byte // Not used for now
}

func (hd Header) String() string {
	ret := ""
	ret = fmt.Sprintf(`%s --------
  EventType   : %s
  ServerID    : %d
  Eventlength : %d
  NextPosition: %d
  Flags       : %v
`, hd.Timestamp.Format(layout), hd.Typecode, hd.ServerID, hd.Eventlength, hd.NextPosition, hd.Flags)
	return ret
}

// event_data
type Ibody interface {
	GetType() string
}

// UNKNOWN_EVENT
type UnknownEvent struct {
	id int
}
func (ue UnknownEvent) GetType() string {
	return "UnknownEvent"
}

// START_EVENT_V3
// QUERY_EVENT
type QueryEvent struct {
	id int
}
func (qe QueryEvent) GetType() string {
	return "QueryEvent"
}

// STOP_EVENT
// ROTATE_EVENT
// INTVAR_EVENT
// LOAD_EVENT
// SLAVE_EVENT
// CREATE_FILE_EVENT
// APPEND_BLOCK_EVENT
// EXEC_LOAD_EVENT
// DELETE_FILE_EVENT
// NEW_LOAD_EVENT
// RAND_EVENT
// USER_VAR_EVENT
// FORMAT_DESCRIPTION_EVENT
type FormatDescriptionEvent struct {
	BinlogEvent int
	ServerVersion string
	CreateTimeStamp time.Time
	HeaderLength int
	PostHeaderLength []byte
	// PostHeaderLength []int
}
func (fde FormatDescriptionEvent) GetType() string {
	return "FormatDescriptionEvent"
}

// XID_EVENT
// BEGIN_LOAD_QUERY_EVENT
// EXECUTE_LOAD_QUERY_EVENT
// TABLE_MAP_EVENT
// PRE_GA_WRITE_ROWS_EVENT
// PRE_GA_UPDATE_ROWS_EVENT
// PRE_GA_DELETE_ROWS_EVENT
// WRITE_ROWS_EVENT
// UPDATE_ROWS_EVENT
// DELETE_ROWS_EVENT
// INCIDENT_EVENT
// HEARTBEAT_LOG_EVENT
// IGNORABLE_LOG_EVENT
// ROWS_QUERY_LOG_EVENT
// WRITE_ROWS_EVENT2
// UPDATE_ROWS_EVENT2
// DELETE_ROWS_EVENT2
// GTID_LOG_EVENT
// ANONYMOUS_GTID_LOG_EVENT
// PREVIOUS_GTIDS_LOG_EVENT
// ENUM_END_EVENT
