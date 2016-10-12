package main

import (
	"./file"
	"./types"
	"io"
	"./influx"
	"log"
	"time"
)

const (
	url = "http://localhost:8086"
	dbName = "analytics"
	username = "bubba"
	password = "bumblebeetuna"
	batchSize = 50000
)

func main() {


	// connect to mysqld and cassandra
	f := file.GetFile("/data/views_yt.csv")
	c := influx.GetInflux(url, username, password)

	// get the batch
	bp := influx.GetBatch(dbName)

	// row for row, extract and insert
	start := time.Now()
	var count uint64 = 0
	var total uint64 = 0
	for { // FYI: set net_read_timeout on mysql

		// read in the row
		line, err := f.Read()

		// Stop at EOF.
		if err == io.EOF {
			break
		}

		// grab the next row
		pp := types.CloneFromCSV(line)
		count +=1
		total +=1

		// Create a point and add to batch
		tags := map[string]string{
			"video_id" : pp.VideoID,
		}
		fields := map[string]interface{}{
			"id" : pp.ID, "views" : pp.Views, "likes" : pp.Likes, "dislikes" : pp.Dislikes,
			"estimated_minutes_watched" : pp.EstimatedMinutesWatched,
			"average_view_duration" : pp.AverageViewDuration,
			"average_view_percentage" : pp.AverageViewPercentage,
			"favorites_added" : pp.FavoritesAdded, "favorites_removed" : pp.FavoritesRemoved,
			"annotation_close_rate" : pp.AnnotationCloseRate,
			"annotation_click_through_rate" : pp.AnnotationClickThroughRate,
			"subscribers_gained" : pp.SubscribersGained, "subscribers_lost" : pp.SubscribersLost,
			"shares" : pp.Shares, "comments" : pp.Comments, "video_id" : pp.VideoID,
			"uniques" : pp.Uniques, "uniques_7day" : pp.Uniques7day, "uniques_30day" : pp.Uniques30day,
		}
		pt := influx.GetPoint("post_metrics", tags, fields, pp.Day)
		bp.AddPoint(pt)

		// commit / log the batch at the right point
		if count == batchSize {
			elapsed := time.Since(start)
			c.Write(bp)
			log.Printf("commmited: %d entries in %s.", total, elapsed)
			time.Sleep(10*time.Second)
			count = 0
		}
	}

	// commit / log any remaining records
	if count % batchSize > 0 {
		elapsed := time.Since(start)
		c.Write(bp)
		log.Printf("commmited: %d entries in %s.", total, elapsed)
	}

}
