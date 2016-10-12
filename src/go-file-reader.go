package main

import (
	"encoding/csv"
	"os"
	"bufio"
	"io"
	"fmt"
	"log"
)

func main () {
	path, err := os.Getwd()
	f, err := os.Open(path + "/data/views_yt.csv")
	if err != nil {
		log.Fatalln("error reading file: ", err)
	}
	r := csv.NewReader(bufio.NewReader(f))
	var count uint64 = 0
	for {
		record, err := r.Read()
		// Stop at EOF.
		if err == io.EOF {
			break
		}
		fmt.Println(record[0])
		for value := range record {
			fmt.Printf("  %v\n", record[value])
		}
		if count == 1 {
			break
		}
		count += 1
	}


}
