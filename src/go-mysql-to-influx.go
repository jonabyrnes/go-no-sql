package main

import (
	"log"
	"time"
	"./mysql"
	"./types"
	"./influx"
	"strconv"
)

const (
	url = "http://localhost:8086"
	dbName = "analytics"
	username = "bubba"
	password = "bumblebeetuna"
	limit = 0
	batchSize = 50000
)

func main() {

	// connect to mysqld and cassandra
	db := mysql.GetDatabase("root:root@/analytics")
	c := influx.GetInflux(url, username, password)

	// mysql query for source data
	// TODO: updated
	metricsQuery := `SELECT id, day, views, likes, dislikes, estimatedMinutesWatched, averageViewDuration,
				averageViewPercentage, favoritesAdded, favoritesRemoved, annotationCloseRate,
				annotationClickThroughRate, subscribersGained, subscribersLost, shares, comments,
				video_id, uniques, uniques_7day, uniques_30day
			FROM analytics.views_yt`

	if( limit > 0) {
		metricsQuery += ` LIMIT ` + strconv.Itoa(limit)
	}

	// execute the mysql query
	start := time.Now()
	rows := mysql.SqlQuery( db, metricsQuery )
	defer rows.Close()

	// get the batch
	bp := influx.GetBatch(dbName)

	// row for row, extract and insert
	var count uint64 = 0
	var total uint64 = 0
	for rows.Next() { // FYI: set net_read_timeout on mysql

		// grab the next row
		count +=1
		total +=1
		dbpp := types.DBPostPoint{}
		if err := rows.Scan( &dbpp.ID, &dbpp.Day, &dbpp.Views, &dbpp.Likes, &dbpp.Dislikes, &dbpp.EstimatedMinutesWatched, &dbpp.AverageViewDuration,
			&dbpp.AverageViewPercentage, &dbpp.FavoritesAdded, &dbpp.FavoritesRemoved, &dbpp.AnnotationCloseRate,
			&dbpp.AnnotationClickThroughRate, &dbpp.SubscribersGained, &dbpp.SubscribersLost, &dbpp.Shares, &dbpp.Comments,
			&dbpp.VideoID, &dbpp.Uniques, &dbpp.Uniques7day, &dbpp.Uniques30day ); err != nil {
			log.Print("error loading sql row : ")
			log.Fatal(err)
		}

		// serialize to something more normal
		pp := types.CloneFromDB(dbpp)

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
			time.Sleep(30*time.Second)
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
