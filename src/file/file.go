package file

import (
	"os"
	"log"
	"encoding/csv"
	"bufio"
)

func GetFile(path string) *csv.Reader {
	cur, err := os.Getwd()
	f, err := os.Open(cur + path)
	if err != nil {
		log.Fatalln("error reading file: ", err)
	}
	r := csv.NewReader(bufio.NewReader(f))
	return r
}