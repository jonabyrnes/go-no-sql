package main

import (
	"log"
	"fmt"
	"time"
	"./mysql"
	"./cassandra"
)

func GetKeysString(list []mysql.SqlKeyValue) string {
	str := ""
	for index, pair := range list {
		if( index > 0 ) {
			str += ","
		}
		str += "'" + pair.Key + "'"
	}
	return str
}

func main() {

	// connect to mysqld and cassandra
	db := mysql.GetDatabase("root:root@/analytics")
	session := cassandra.GetCassandra("127.0.0.1", "analytics")
	defer session.Close()

	// get the groups by top level network
	groups := mysql.GetSqlKeyValues(db,
		`SELECT CONVERT(groups.id, CHAR(7)), groups.name
		FROM groups_groups
		JOIN groups ON groups.id = groups_groups.child_group_id
		WHERE parent_group_id = (
			SELECT id FROM groups WHERE name = 'DDN' and group_type_id = 4
		) AND group_type_id = 7
		ORDER BY name`)

	// get video ids for the groups
	log.Println("video ids")
	videos := mysql.GetSqlKeyValues(db, fmt.Sprintf(
		`SELECT CONVERT(video_id, CHAR(7)), CONVERT(group_id, CHAR(7))
		FROM videos_groups
		WHERE group_id in (%s)`, GetKeysString(groups)))

	log.Println("get metrics")
	// get the metrics for all videos on a given date
	start := time.Now()
	posts := cassandra.GetCqlKeyValues(session, fmt.Sprintf(
		`SELECT day, post_id, metrics
		FROM analytics.post_metrics
		WHERE day = '%s' AND post_id in (%s)`, "2016-04-01", GetKeysString(videos[0:10000])))
	elapsed := time.Since(start)
	log.Printf("%s", elapsed)

	for _, post := range posts {
		log.Print(post.Value)
	}

	// top views + total views by network
	// select the post id's based on group (network) criteria
	// batch the ids to be safe
	// select and build an in memory table
	// gather the stats based on the memory table
}
