package main

import (
	"github.com/influxdata/influxdb/client/v2"
	"log"
	//"time"
	//"encoding/json"
)

const (
	MyDB = "analytics"
	username = "bubba"
	password = "bumblebeetuna"
)

// queryDB convenience function to query the database
func queryDB(clnt client.Client, cmd string) (res []client.Result, err error) {
	q := client.Query{
		Command:  cmd,
		Database: MyDB,
	}
	if response, err := clnt.Query(q); err == nil {
		if response.Error() != nil {
			return res, response.Error()
		}
		res = response.Results
	} else {
		return res, err
	}
	return res, nil
}

func main() {

	// Make client
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr: "http://localhost:8086",
		Username: username,
		Password: password,
	})

	if err != nil {
		log.Fatalln("Error: ", err)
	}


	q := "select sum(views) from post_metrics group by video_id"
	var res, _ = queryDB(c, q)
	if err != nil {
		log.Fatal(err)
	}

	for _, row := range res[0].Series {
		videoId := row.Tags["video_id"]
		sum := row.Values[0][1]
		log.Printf("%s: %s\n", videoId, sum)
	}
}

