package influx

import (
	"log"
	"github.com/influxdata/influxdb/client/v2"
	"time"
)

func GetInflux(url string, user string, pass string) client.Client {
	c, err := client.NewHTTPClient(client.HTTPConfig {
		Addr: url,
		Username: user,
		Password: pass,
	})
	if err != nil {
		log.Fatalln("influx error : ", err)
	}
	return c
}

func GetBatch(db string) client.BatchPoints {
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  db,
		Precision: "s",
	})
	if err != nil {
		log.Fatalln("influx error : ", err)
	}
	return bp
}

func GetPoint(measurement string, tags map[string]string, fields map[string]interface{}, time time.Time) *client.Point {
	pt, err := client.NewPoint(measurement, tags, fields, time)
	if err != nil {
		log.Fatalln("influx error : ", err)
	}
	return pt
}
