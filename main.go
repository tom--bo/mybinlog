package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"reflect"
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

func printEvents(events []Event) {
	for _, e := range events {
		if e.Header.Typecode != UNKNOWN_EVENT {
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
