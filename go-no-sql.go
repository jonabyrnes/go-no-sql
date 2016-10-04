package main

import (
	"log"
	"database/sql"
	"github.com/gocql/gocql"
	"encoding/json"
	"time"
	//"github.com/influxdata/influxdb/client"
	"github.com/go-sql-driver/mysql"
)

type DBPostPoint struct {
	ID			string
	Day			mysql.NullTime
	Views			sql.NullInt64
	Likes			sql.NullInt64
	Dislikes		sql.NullInt64
	EstimatedMinutesWatched	sql.NullFloat64
	AverageViewDuration	sql.NullFloat64
	AverageViewPercentage	sql.NullFloat64
	FavoritesAdded		sql.NullInt64
	FavoritesRemoved	sql.NullInt64
	AnnotationCloseRate	sql.NullFloat64
	AnnotationClickThroughRate	sql.NullFloat64
	SubscribersGained	sql.NullInt64
	SubscribersLost		sql.NullInt64
	Shares			sql.NullInt64
	Comments 		sql.NullInt64
	VideoID			sql.NullString
	Uniques			sql.NullInt64
	Uniques7day		sql.NullInt64
	Uniques30day		sql.NullInt64
}

type PostPoint struct {
	ID			string		`json:"id"`
	Day			time.Time	`json:"day"`
	Views			int64		`json:"views"`
	Likes			int64		`json:"likes"`
	Dislikes		int64		`json:"dislikes"`
	EstimatedMinutesWatched	float64		`json:"estimatedMinutesWatched"`
	AverageViewDuration	float64		`json:"averageViewDuration"`
	AverageViewPercentage	float64		`json:"averageViewPercentage"`
	FavoritesAdded		int64		`json:"favoritesAdded"`
	FavoritesRemoved	int64		`json:"favoritesRemoved"`
	AnnotationCloseRate	float64		`json:"annotationCloseRate"`
	AnnotationClickThroughRate	float64	`json:"annotationClickThroughRate"`
	SubscribersGained	int64		`json:"subscribersGained"`
	SubscribersLost		int64		`json:"subscribersLost"`
	Shares			int64		`json:"shares"`
	Comments 		int64		`json:"comments"`
	VideoID			string		`json:"video_id"`
	Uniques			int64		`json:"uniques"`
	Uniques7day		int64		`json:"uniques_7day"`
	Uniques30day		int64		`json:"uniques_30day"`
}

func CloneFromDB(dbpp DBPostPoint) PostPoint {
	pp := PostPoint {
		ID : dbpp.ID, Day : dbpp.Day.Time, Views : dbpp.Views.Int64, Likes : dbpp.Likes.Int64, Dislikes : dbpp.Dislikes.Int64,
		EstimatedMinutesWatched	: dbpp.EstimatedMinutesWatched.Float64,
		AverageViewDuration : dbpp.AverageViewDuration.Float64,
		AverageViewPercentage : dbpp.AverageViewPercentage.Float64,
		FavoritesAdded : dbpp.FavoritesAdded.Int64, FavoritesRemoved : dbpp.FavoritesRemoved.Int64,
		AnnotationCloseRate : dbpp.AnnotationCloseRate.Float64, AnnotationClickThroughRate : dbpp.AnnotationClickThroughRate.Float64,
		SubscribersGained : dbpp.SubscribersGained.Int64, SubscribersLost : dbpp.SubscribersLost.Int64, Shares : dbpp.Shares.Int64,
		Comments : dbpp.Comments.Int64, VideoID : dbpp.VideoID.String,
		Uniques : dbpp.Uniques.Int64, Uniques7day : dbpp.Uniques7day.Int64, Uniques30day : dbpp.Uniques30day.Int64,
	}
	return pp
}

func main() {

	// connect to mysql and cassandra
    	db, err := sql.Open("mysql", "root:root@/analytics")
	cluster := gocql.NewCluster("127.0.0.1")
	cluster.ProtoVersion = 4
	cluster.Keyspace = "analytics"
	cluster.Consistency = gocql.One
	cluster.Timeout = 2 * time.Minute
	cluster.NumConns = 1
	cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: 5}
	session, _ := cluster.CreateSession()
	defer session.Close()

	// TODO use arrays for dynamic variables/columns, etc
	// updated
	points_query := `SELECT id, day, views, likes, dislikes, estimatedMinutesWatched, averageViewDuration,
				averageViewPercentage, favoritesAdded, favoritesRemoved, annotationCloseRate,
				annotationClickThroughRate, subscribersGained, subscribersLost, shares, comments,
				video_id, uniques, uniques_7day, uniques_30day
			FROM analytics.views_yt`

	// execute the mysql query
	start := time.Now()
	rows, err := db.Query(points_query)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	// row for row, extract and insert
	var count uint64 = 0
	for rows.Next() {
		dbpp := DBPostPoint{}
		if err := rows.Scan( &dbpp.ID, &dbpp.Day, &dbpp.Views, &dbpp.Likes, &dbpp.Dislikes, &dbpp.EstimatedMinutesWatched, &dbpp.AverageViewDuration,
			&dbpp.AverageViewPercentage, &dbpp.FavoritesAdded, &dbpp.FavoritesRemoved, &dbpp.AnnotationCloseRate,
			&dbpp.AnnotationClickThroughRate, &dbpp.SubscribersGained, &dbpp.SubscribersLost, &dbpp.Shares, &dbpp.Comments,
			&dbpp.VideoID, &dbpp.Uniques, &dbpp.Uniques7day, &dbpp.Uniques30day ); err != nil {
			log.Println("mysql error")
			log.Fatal(err)
		}

		pp := CloneFromDB(dbpp)
		p, _ := json.Marshal(pp)
		log.Println(string(p))

		points_insert := `INSERT INTO analytics.post_points( id, day, views, likes, dislikes, estimatedMinutesWatched, averageViewDuration,
					averageViewPercentage, favoritesAdded, favoritesRemoved, annotationCloseRate,
					annotationClickThroughRate, subscribersGained, subscribersLost, shares, comments,
					video_id, uniques, uniques_7day, uniques_30day )
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

		// insert the point
		if err := session.Query(points_insert,
			pp.ID, pp.Day, pp.Views, pp.Likes, pp.Dislikes, pp.EstimatedMinutesWatched, pp.AverageViewDuration,
			pp.AverageViewPercentage, pp.FavoritesAdded, pp.FavoritesRemoved, pp.AnnotationCloseRate,
			pp.AnnotationClickThroughRate, pp.SubscribersGained, pp.SubscribersLost, pp.Shares, pp.Comments,
			pp.VideoID, pp.Uniques, pp.Uniques7day, pp.Uniques30day).Exec(); err != nil {
			log.Println("cassandra error")
			log.Println(err)
		}
		if( count == 2 ) {
			//time.Sleep(500 * time.Millisecond);
			count = 0;
		}
		count+=1
	}

	if err := rows.Err(); err != nil {
		log.Println("fatal cassandra error")
		log.Fatal(err)
	}
	elapsed := time.Since(start)
	log.Printf("done. %s", elapsed)
}
