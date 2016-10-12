package main

import (
	"log"
	"encoding/json"
	"time"
	"strings"
	"./cassandra"
	"./mysql"
	"./types"
	"strconv"
)

const (
	limit = 1000
	batchSize = 1000
)

func main() {

	// connect to mysqld and cassandra
	db := mysql.GetDatabase("root:root@/analytics")
	session := cassandra.GetCassandra("127.0.0.1", "analytics")
	defer session.Close()

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

	// row for row, extract and insert
	var count uint64 = 0
	var total uint64 = 0
	for rows.Next() {

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

		// generate the insert
		metrics, _ := json.Marshal(pp)
		date := pp.Day.Format(time.RFC3339);
		day := strings.Split(date,"T")[0] // technically this may not be correct
		metricsInsert := `INSERT INTO analytics.post_metrics( post_id, day, metrics ) VALUES (?, ?, ?)`
		//if err := session.Query(metrics_insert, pp.VideoID, pp.Day, metrics).Exec(); err != nil {
		if err := session.Query(metricsInsert, pp.VideoID, day, metrics).Exec(); err != nil {
			log.Print("cassandra error : ")
			log.Fatal(err)
		}

		// commit / log the batch at the right point
		if count == batchSize {
			elapsed := time.Since(start)
			log.Printf("commmited: %d entries in %s.", total, elapsed)
			count = 0
		}
	}

	// commit / log any remaining records
	if count % batchSize > 0 {
		elapsed := time.Since(start)
		log.Printf("commmited: %d entries in %s.", total, elapsed)
	}

}
