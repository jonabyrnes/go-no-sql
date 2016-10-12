package types

import (
	"time"
	"database/sql"
	"github.com/go-sql-driver/mysql"
	"strconv"
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
	VideoID			string		`json:"videoId"`
	Uniques			int64		`json:"uniques"`
	Uniques7day		int64		`json:"uniques7day"`
	Uniques30day		int64		`json:"uniques30day"`
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

func CloneFromCSV(line []string) PostPoint {
	pp := PostPoint{}
	if line[0] != `\N` { pp.ID = line[0] }
	if line[1] != `\N` {
		day, _ := time.Parse(time.RFC3339, line[1] + "T04:00:00+00:00")
		pp.Day = day
	}
	if line[2] != `\N` {
		views, _ := strconv.ParseInt(line[2], 10, 32)
		pp.Views = views
	}
	if line[3] != `\N` {
		likes, _ := strconv.ParseInt(line[3], 10, 32)
		pp.Likes = likes
	}
	if line[4] != `\N` {
		dislikes, _ := strconv.ParseInt(line[4], 10, 32)
		pp.Dislikes = dislikes
	}
	if line[5] != `\N` {
		estimatedMinutesWatched, _ := strconv.ParseFloat(line[5], 32)
		pp.EstimatedMinutesWatched = estimatedMinutesWatched
	}
	if line[6] != `\N` {
		averageViewDuration, _ := strconv.ParseFloat(line[6], 32)
		pp.AverageViewDuration = averageViewDuration
	}
	if line[7] != `\N` {
		averageViewPercentage, _ := strconv.ParseFloat(line[7], 32)
		pp.AverageViewPercentage = averageViewPercentage
	}
	if line[8] != `\N` {
		favoritesAdded, _ := strconv.ParseInt(line[8], 10, 32)
		pp.FavoritesAdded = favoritesAdded
	}
	if line[9] != `\N` {
		favoritesRemoved, _ := strconv.ParseInt(line[9], 10, 32)
		pp.FavoritesRemoved = favoritesRemoved
	}
	if line[10] != `\N` {
		annotationCloseRate, _ := strconv.ParseFloat(line[10], 32)
		pp.AnnotationCloseRate = annotationCloseRate
	}
	if line[11] != `\N` {
		annotationClickThroughRate, _ := strconv.ParseFloat(line[11], 32)
		pp.AnnotationClickThroughRate = annotationClickThroughRate
	}
	if line[12] != `\N` {
		subscribersGained, _ := strconv.ParseInt(line[12], 10, 32)
		pp.SubscribersGained = subscribersGained
	}
	if line[13] != `\N` {
		subscribersLost, _ := strconv.ParseInt(line[13], 10, 32)
		pp.SubscribersLost = subscribersLost
	}
	if line[14] != `\N` {
		shares, _ := strconv.ParseInt(line[14], 10, 32)
		pp.Shares = shares
	}
	if line[15] != `\N` {
		comments, _ := strconv.ParseInt(line[15], 10, 32)
		pp.Comments = comments
	}
	if line[16] != `\N` { pp.VideoID = line[16] }
	// TODO: updated
	//if line[17] != `\N` {
	//	day, _ := time.Parse(time.RFC3339, line[17] + "T04:00:00+00:00")
	//}
	if line[18] != `\N` {
		uniques, _ := strconv.ParseInt(line[18], 10, 32)
		pp.Uniques = uniques
	}
	if line[19] != `\N` {
		uniques7day, _ := strconv.ParseInt(line[19], 10, 32)
		pp.Uniques7day = uniques7day
	}
	if line[20] != `\N` {
		uniques30day, _ := strconv.ParseInt(line[20], 10, 32)
		pp.Uniques30day = uniques30day
	}
	return pp
}