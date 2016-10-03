package main

import (
	_ "github.com/go-sql-driver/mysql"
	"log"
	"database/sql"
	"github.com/gocql/gocql"
	"github.com/go-sql-driver/mysql"
	"encoding/json"
	"time"
)

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

	type VideoPoint struct {
		ID			string			`json:"id"`
		Day			mysql.NullTime		`json:"day"`
		Views			sql.NullInt64		`json:"views"`
		Likes			sql.NullInt64		`json:"likes"`
		Dislikes		sql.NullInt64		`json:"dislikes"`
		EstimatedMinutesWatched	sql.NullFloat64		`json:"estimatedMinutesWatched"`
		AverageViewDuration	sql.NullFloat64		`json:"averageViewDuration"`
		AverageViewPercentage	sql.NullFloat64		`json:"averageViewPercentage"`
		FavoritesAdded		sql.NullInt64		`json:"favoritesAdded"`
		FavoritesRemoved	sql.NullInt64		`json:"favoritesRemoved"`
		AnnotationCloseRate	sql.NullFloat64		`json:"annotationCloseRate"`
		AnnotationClickThroughRate	sql.NullFloat64	`json:"annotationClickThroughRate"`
		SubscribersGained	sql.NullInt64		`json:"subscribersGained"`
		SubscribersLost		sql.NullInt64		`json:"subscribersLost"`
		Shares			sql.NullInt64		`json:"shares"`
		Comments 		sql.NullInt64		`json:"comments"`
		VideoID			sql.NullString		`json:"video_id"`
		Uniques			sql.NullInt64		`json:"uniques"`
		Uniques7day		sql.NullInt64		`json:"uniques_7day"`
		Uniques30day		sql.NullInt64		`json:"uniques_30day"`
	}

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
		vp := VideoPoint{}
		if err := rows.Scan( &vp.ID, &vp.Day, &vp.Views, &vp.Likes, &vp.Dislikes, &vp.EstimatedMinutesWatched, &vp.AverageViewDuration,
			&vp.AverageViewPercentage, &vp.FavoritesAdded, &vp.FavoritesRemoved, &vp.AnnotationCloseRate,
			&vp.AnnotationClickThroughRate, &vp.SubscribersGained, &vp.SubscribersLost, &vp.Shares, &vp.Comments,
			&vp.VideoID, &vp.Uniques, &vp.Uniques7day, &vp.Uniques30day ); err != nil {
			log.Println("mysql error")
			log.Fatal(err)
		}

		p, _ := json.Marshal(vp)
		log.Println(string(p))

		points_insert := `INSERT INTO analytics.post_points( id, day, views, likes, dislikes, estimatedMinutesWatched, averageViewDuration,
					averageViewPercentage, favoritesAdded, favoritesRemoved, annotationCloseRate,
					annotationClickThroughRate, subscribersGained, subscribersLost, shares, comments,
					video_id, uniques, uniques_7day, uniques_30day )
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

		// insert the point
		if err := session.Query(points_insert,
			vp.ID, vp.Day.Time, vp.Views.Int64, vp.Likes.Int64, vp.Dislikes.Int64, vp.EstimatedMinutesWatched.Float64, vp.AverageViewDuration.Float64,
			vp.AverageViewPercentage.Float64, vp.FavoritesAdded.Int64, vp.FavoritesRemoved.Int64, vp.AnnotationCloseRate.Float64,
			vp.AnnotationClickThroughRate.Float64, vp.SubscribersGained.Int64, vp.SubscribersLost.Int64, vp.Shares.Int64, vp.Comments.Int64,
			vp.VideoID.String, vp.Uniques.Int64, vp.Uniques7day.Int64, vp.Uniques30day.Int64).Exec(); err != nil {
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
