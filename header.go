package main

import (
	"fmt"
	"time"
)

const layout = "2006-01-02 15:04:05"

type LogEventType int

type Counter struct {
	total   int
	success int
	unknown int
	error   int
	event   map[LogEventType]int
}

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

func (lt LogEventType) String() string {
	switch lt {
	case UNKNOWN_EVENT:
		return "UNKNOWN_EVENT"
	case START_EVENT_V3:
		return "START_EVENT_V3"
	case QUERY_EVENT:
		return "QUERY_EVENT"
	case STOP_EVENT:
		return "STOP_EVENT"
	case ROTATE_EVENT:
		return "ROTATE_EVENT"
	case INTVAR_EVENT:
		return "INTVAR_EVENT"
	case LOAD_EVENT:
		return "LOAD_EVENT"
	case SLAVE_EVENT:
		return "SLAVE_EVENT"
	case CREATE_FILE_EVENT:
		return "CREATE_FILE_EVENT"
	case APPEND_BLOCK_EVENT:
		return "APPEND_BLOCK_EVENT"
	case EXEC_LOAD_EVENT:
		return "EXEC_LOAD_EVENT"
	case DELETE_FILE_EVENT:
		return "DELETE_FILE_EVENT"
	case NEW_LOAD_EVENT:
		return "NEW_LOAD_EVENT"
	case RAND_EVENT:
		return "RAND_EVENT"
	case USER_VAR_EVENT:
		return "USER_VAR_EVENT"
	case FORMAT_DESCRIPTION_EVENT:
		return "FORMAT_DESCRIPTION_EVENT"
	case XID_EVENT:
		return "XID_EVENT"
	case BEGIN_LOAD_QUERY_EVENT:
		return "BEGIN_LOAD_QUERY_EVENT"
	case EXECUTE_LOAD_QUERY_EVENT:
		return "EXECUTE_LOAD_QUERY_EVENT"
	case TABLE_MAP_EVENT:
		return "TABLE_MAP_EVENT"
	case PRE_GA_WRITE_ROWS_EVENT:
		return "PRE_GA_WRITE_ROWS_EVENT"
	case PRE_GA_UPDATE_ROWS_EVENT:
		return "PRE_GA_UPDATE_ROWS_EVENT"
	case PRE_GA_DELETE_ROWS_EVENT:
		return "PRE_GA_DELETE_ROWS_EVENT"
	case WRITE_ROWS_EVENT:
		return "WRITE_ROWS_EVENT"
	case UPDATE_ROWS_EVENT:
		return "UPDATE_ROWS_EVENT"
	case DELETE_ROWS_EVENT:
		return "DELETE_ROWS_EVENT"
	case INCIDENT_EVENT:
		return "INCIDENT_EVENT"
	case HEARTBEAT_LOG_EVENT:
		return "HEARTBEAT_LOG_EVENT"
	case IGNORABLE_LOG_EVENT:
		return "IGNORABLE_LOG_EVENT"
	case ROWS_QUERY_LOG_EVENT:
		return "ROWS_QUERY_LOG_EVENT"
	case WRITE_ROWS_EVENT2:
		return "WRITE_ROWS_EVENT2"
	case UPDATE_ROWS_EVENT2:
		return "UPDATE_ROWS_EVENT2"
	case DELETE_ROWS_EVENT2:
		return "DELETE_ROWS_EVENT2"
	case GTID_LOG_EVENT:
		return "GTID_LOG_EVENT"
	case ANONYMOUS_GTID_LOG_EVENT:
		return "ANONYMOUS_GTID_LOG_EVENT"
	case PREVIOUS_GTIDS_LOG_EVENT:
		return "PREVIOUS_GTIDS_LOG_EVENT"
	case ENUM_END_EVENT:
		return "ENUM_END_EVENT"
	}
	return "UNKNOWN_EVENT"
}

type Event struct {
	Header Header
	Body   Ibody // event_data
}

type Header struct {
	Timestamp    time.Time
	Typecode     LogEventType
	ServerID     int
	Eventlength  int
	NextPosition int
	Flags        int
	ExtraHeader  []byte // Not used for now
}

func (hd Header) String() string {
	ret := ""
	ret = fmt.Sprintf(`%s --------
[Header]
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

func (a UnknownEvent) GetType() string {
	return "UnknownEvent"
}

// START_EVENT_V3 (Not support, not used in MySQL >= 5.0)
type StartEventV3 struct {
}

func (a StartEventV3) GetType() string {
	return "StartEventV3"
}

// QUERY_EVENT
type QueryEvent struct {
	ThreadID        int
	ExecutionTime   int
	DBNameLen       int
	ErrorCode       int
	StatusVarLen    int
	StatusVariables StatusVariable
	DatabaseName    string
	SQLStatement    string
}

func (a QueryEvent) GetType() string {
	return "QueryEvent"
}

type StatusVariable struct {
	Flags2            []byte // not supported (only as of 5.0)
	SQLMode           []byte
	Catalog           string // not supported (only 5.0.0 to 5.0.3)
	AutoIncrement1    uint16
	AutoIncrement2    uint16 // auto_increment_offset
	Charset1          uint16 // character_set_client
	Charset2          uint16 // collation_connection
	Charset3          uint16 // collation_server
	Timezone          string
	CatalogNZ         string
	LCTimeNames       uint16 //lc_time_names
	CharsetDatabase   uint16 // collation_database
	TableMapForUpdate []byte
}

// STOP_EVENT
type StopEvent struct {
}

func (a StopEvent) GetType() string {
	return "StopEvent"
}

// ROTATE_EVENT
type RotateEvent struct {
	NextPos  int
	NextName string
}

func (a RotateEvent) GetType() string {
	return "RotateEvent"
}

type IntVarOpt int

const (
	LAST_INSERT_ID_EVENT IntVarOpt = 1
	INSERT_ID_EVENT      IntVarOpt = 2
)

func (ivo IntVarOpt) String() string {
	switch ivo {
	case LAST_INSERT_ID_EVENT:
		return "LAST_INSERT_ID_EVENT"
	case INSERT_ID_EVENT:
		return "INSERT_ID_EVENT"
	}
	return "UNKNOWN"
}

// INTVAR_EVENT
type IntVar struct {
	Opt1  IntVarOpt // 1: LAST_INSERT_ID_EVENT, 2: INSERT_ID_EVENT
	Value int       // LAST_INSERT_ID() or AUTO_INCREMENT
}

func (a IntVar) GetType() string {
	return "IntVar"
}

// LOAD_EVENT
type LoadEvent struct {
}

func (a LoadEvent) GetType() string {
	return "LoadEvent"
}

// SLAVE_EVENT (Never used)
type SlaveEvent struct {
}

func (a SlaveEvent) GetType() string {
	return "SlaveEvent"
}

// CREATE_FILE_EVENT
type CreateFileEvent struct {
}

func (a CreateFileEvent) GetType() string {
	return "CreateFileEvent"
}

// APPEND_BLOCK_EVENT
type AppendBlock struct {
	ID   int
	Data []byte
}

func (a AppendBlock) GetType() string {
	return "AppendBlock"
}

// EXEC_LOAD_EVENT
type ExecLoad struct {
	ID int
}

func (a ExecLoad) GetType() string {
	return "ExecLoad"
}

// DELETE_FILE_EVENT
type DeleteFile struct {
	ID int
}

func (a DeleteFile) GetType() string {
	return "DeleteFile"
}

// NEW_LOAD_EVENT
type NewLoadEvent struct {
}

func (a NewLoadEvent) GetType() string {
	return "NewLoadEvent"
}

// RAND_EVENT
type Rand struct {
	FirstSeed  int
	SecondSeed int
}

func (a Rand) GetType() string {
	return "Rand"
}

// USER_VAR_EVENT
type UserVarEvent struct {
}

func (a UserVarEvent) GetType() string {
	return "UserVarEvent"
}

// FORMAT_DESCRIPTION_EVENT
type FormatDescriptionEvent struct {
	BinlogEvent      int
	ServerVersion    string
	CreateTimeStamp  time.Time
	HeaderLength     int
	PostHeaderLength []byte
	// PostHeaderLength []int
}

func (a FormatDescriptionEvent) GetType() string {
	return "FormatDescriptionEvent"
}

// XID_EVENT
type XID struct {
	XID int
}

func (a XID) GetType() string {
	return "XID"
}

// BEGIN_LOAD_QUERY_EVENT
type BeginLoadQuery struct {
	ID   int
	Data []byte
}

func (a BeginLoadQuery) GetType() string {
	return "BeginLoadQuery"
}

// EXECUTE_LOAD_QUERY_EVENT
type ExecuteLoadQueryEvent struct {
}

func (a ExecuteLoadQueryEvent) GetType() string {
	return "ExecuteLoadQueryEvent"
}

// TABLE_MAP_EVENT
type TableMapEvent struct {
	TableID      int
	ReservedByte []byte
	DBNameLen    int
	DBName       string
	TableNameLen int
	TableName    string
	NumOfCol     int
	ColType      []byte
	MetaBlockLen int
	MetaBlock    []byte
	IsNull       []byte
}

func (a TableMapEvent) GetType() string {
	return "TableMapEvent"
}

// PRE_GA_WRITE_ROWS_EVENT (not support, only in 5.1.5 to 5.1.17)
type PreGAWriteRows struct {
}

func (a PreGAWriteRows) GetType() string {
	return "PreGAWriteRows"
}

// PRE_GA_UPDATE_ROWS_EVENT (not support, only in 5.1.5 to 5.1.17)
type PreGAUpdateRows struct {
}

func (a PreGAUpdateRows) GetType() string {
	return "PreGAUpdateRows"
}

// PRE_GA_DELETE_ROWS_EVENT (not support, only in 5.1.5 to 5.1.17)
type PreGADeleteRows struct {
}

func (a PreGADeleteRows) GetType() string {
	return "PreGADeleteRows"
}

// WRITE_ROWS_EVENT
type WriteRows struct {
	TableID      int
	ReservedByte []byte
	NumOfCol     int
	IsUsed       []byte
	IsNull       []byte
	AfterImage   []byte // todo
}

func (a WriteRows) GetType() string {
	return "WriteRows"
}

// UPDATE_ROWS_EVENT
type UpdateRows struct {
	TableID      int
	ReservedByte []byte
	NumOfCol     int
	IsUsedBefore []byte
	IsUsedAfter  []byte
	IsNull       []byte
	BeforeImage  []byte // todo
	AfterImage   []byte // todo
}

func (a UpdateRows) GetType() string {
	return "UpdateRows"
}

// DELETE_ROWS_EVENT
type DeleteRows struct {
	TableID      int
	ReservedByte []byte
	NumOfCol     int
	IsUsed       []byte
	IsNull       []byte
	AfterImage   []byte // todo
}

func (a DeleteRows) GetType() string {
	return "DeleteRows"
}

// INCIDENT_EVENT
type Incident struct {
	IncidentNum int
	MessageLen  int
	Message     string
}

func (a Incident) GetType() string {
	return "Incident"
}

// HEARTBEAT_LOG_EVENT (not used now)
type HeartbeatLog struct {
}

func (a HeartbeatLog) GetType() string {
	return "HeartbeatLog"
}

// IGNORABLE_LOG_EVENT
type IgnorableLogEvent struct {
}

func (a IgnorableLogEvent) GetType() string {
	return "IgnorableLogEvent"
}

// ROWS_QUERY_LOG_EVENT
type RowsQueryLogEvent struct {
}

func (a RowsQueryLogEvent) GetType() string {
	return "RowsQueryLogEvent"
}

// WRITE_ROWS_EVENT2
type WriteRowsEvent2 struct {
}

func (a WriteRowsEvent2) GetType() string {
	return "WriteRowsEvent2"
}

// UPDATE_ROWS_EVENT2
type UpdateRowsEvent2 struct {
}

func (a UpdateRowsEvent2) GetType() string {
	return "UpdateRowsEvent2"
}

// DELETE_ROWS_EVENT2
type DeleteRowsEvent2 struct {
}

func (a DeleteRowsEvent2) GetType() string {
	return "DeleteRowsEvent2"
}

// GTID_LOG_EVENT
type GtidLogEvent struct {
}

func (a GtidLogEvent) GetType() string {
	return "GtidLogEvent"
}

// ANONYMOUS_GTID_LOG_EVENT
type AnonymousGtidLogEvent struct {
}

func (a AnonymousGtidLogEvent) GetType() string {
	return "AnonymousGtidLogEvent"
}

// PREVIOUS_GTIDS_LOG_EVENT
type PreviousGtidsLogEvent struct {
}

func (a PreviousGtidsLogEvent) GetType() string {
	return "PreviousGtidsLogEvent"
}

// ENUM_END_EVENT
type EnumEndEvent struct {
}

func (a EnumEndEvent) GetType() string {
	return "EnumEndEvent"
}
