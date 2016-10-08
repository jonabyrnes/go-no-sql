package types

import (
	"time"
	"database/sql"
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