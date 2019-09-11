package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"reflect"
)

var (
	doPrint     bool
	doPrintJSON bool
	filterEvent string
)

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

func printCount(c Counter) {
	fmt.Println("totalCount: ", c.total)
	fmt.Println("successCount: ", c.success)
	fmt.Println("errorCount: ", c.error)
	fmt.Println("unknownCount: ", c.unknown)

	fmt.Println("-- Event count -- ")
	for k, v := range c.event {
		fmt.Printf("%25s: %d\n", k, v)
	}
	fmt.Println()
}

func printEvents(filter LogEventType, events []Event) {
	for _, e := range events {
		typeCode := e.Header.Typecode
		if typeCode != UNKNOWN_EVENT && (typeCode == filter || typeCode == ALL_EVENT) {
			if doPrint {
				fmt.Println(e.Header)
				// fmt.Printf("%+v", e.Body)

				fmt.Println("[Body]")
				v := reflect.Indirect(reflect.ValueOf(e.Body))
				t := v.Type()
				for i := 0; i < t.NumField(); i++ {
					fmt.Print("  " + t.Field(i).Name + ": ")
					fmt.Println(v.Field(i))
				}

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

func checkEventFilter(eventName string) (LogEventType, error) {
	switch eventName {
	case "": // no filter if eventName is not specified
		return ALL_EVENT, nil
	case "UNKNOWN_EVENT":
		return UNKNOWN_EVENT, nil
	case "START_EVENT_V3":
		return START_EVENT_V3, nil
	case "QUERY_EVENT":
		return QUERY_EVENT, nil
	case "STOP_EVENT":
		return STOP_EVENT, nil
	case "ROTATE_EVENT":
		return ROTATE_EVENT, nil
	case "INTVAR_EVENT":
		return INTVAR_EVENT, nil
	case "LOAD_EVENT":
		return LOAD_EVENT, nil
	case "SLAVE_EVENT":
		return SLAVE_EVENT, nil
	case "CREATE_FILE_EVENT":
		return CREATE_FILE_EVENT, nil
	case "APPEND_BLOCK_EVENT":
		return APPEND_BLOCK_EVENT, nil
	case "EXEC_LOAD_EVENT":
		return EXEC_LOAD_EVENT, nil
	case "DELETE_FILE_EVENT":
		return DELETE_FILE_EVENT, nil
	case "NEW_LOAD_EVENT":
		return NEW_LOAD_EVENT, nil
	case "RAND_EVENT":
		return RAND_EVENT, nil
	case "USER_VAR_EVENT":
		return USER_VAR_EVENT, nil
	case "FORMAT_DESCRIPTI,ON_EVENT":
		return FORMAT_DESCRIPTION_EVENT, nil
	case "XID_EVENT":
		return XID_EVENT, nil
	case "BEGIN_LOAD_QUERY_EVENT":
		return BEGIN_LOAD_QUERY_EVENT, nil
	case "EXECUTE_LOAD_QUERY_EVENT":
		return EXECUTE_LOAD_QUERY_EVENT, nil
	case "TABLE_MAP_EVENT":
		return TABLE_MAP_EVENT, nil
	case "PRE_GA_WRITE_ROWS_EVENT":
		return PRE_GA_WRITE_ROWS_EVENT, nil
	case "PRE_GA_UPDATE_ROWS_EVENT":
		return PRE_GA_UPDATE_ROWS_EVENT, nil
	case "PRE_GA_DELETE_ROWS_EVENT":
		return PRE_GA_DELETE_ROWS_EVENT, nil
	case "WRITE_ROWS_EVENT":
		return WRITE_ROWS_EVENT, nil
	case "UPDATE_ROWS_EVENT":
		return UPDATE_ROWS_EVENT, nil
	case "DELETE_ROWS_EVENT":
		return DELETE_ROWS_EVENT, nil
	case "INCIDENT_EVENT":
		return INCIDENT_EVENT, nil
	case "HEARTBEAT_LOG_EVENT":
		return HEARTBEAT_LOG_EVENT, nil
	case "IGNORABLE_LOG_EVENT":
		return IGNORABLE_LOG_EVENT, nil
	case "ROWS_QUERY_LOG_EVENT":
		return ROWS_QUERY_LOG_EVENT, nil
	case "WRITE_ROWS_EVENT2":
		return WRITE_ROWS_EVENT2, nil
	case "UPDATE_ROWS_EVENT2":
		return UPDATE_ROWS_EVENT2, nil
	case "DELETE_ROWS_EVENT2":
		return DELETE_ROWS_EVENT2, nil
	case "GTID_LOG_EVENT":
		return GTID_LOG_EVENT, nil
	case "ANONYMOUS_GTID_LOG_EVENT":
		return ANONYMOUS_GTID_LOG_EVENT, nil
	case "PREVIOUS_GTIDS_LOG_EVENT":
		return PREVIOUS_GTIDS_LOG_EVENT, nil
	case "ENUM_END_EVENT":
		return ENUM_END_EVENT, nil
	case "ALL_EVENT":
		return ALL_EVENT, nil
	default:
		return UNKNOWN_EVENT, errors.New("Invalid event!!")
	}

	return UNKNOWN_EVENT, errors.New("Invalid event!!")
}

func main() {
	// flags
	flag.BoolVar(&doPrint, "p", false, "Do print events")
	flag.BoolVar(&doPrintJSON, "j", false, "Do print event headers as JSON")
	flag.StringVar(&filterEvent, "f", "", "Event filter")
	flag.Parse()

	filter, err := checkEventFilter(filterEvent)
	if err != nil {
		fmt.Println(err)
		return
	}

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
		printEvents(filter, events)
	}
}
