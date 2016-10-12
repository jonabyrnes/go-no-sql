package cassandra

import (
	"github.com/gocql/gocql"
	"log"
)

type CqlKeyValue struct {
	RowKey string
	Column string
	Value string
}

func GetCassandra(hosts string, keyspace string) *gocql.Session {
	cluster := gocql.NewCluster(hosts)
	cluster.ProtoVersion = 4
	cluster.Keyspace = keyspace
	cluster.Consistency = gocql.One
	cluster.NumConns = 1
	cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: 5}
	session, _ := cluster.CreateSession()
	return session
}

func GetCqlKeyValues(session *gocql.Session, q string) []CqlKeyValue {
	log.Println(q)
	var (
		rowKey string
		column string
		value string
	)

	list := []CqlKeyValue{}
	iter := session.Query(q).Iter()
	for iter.Scan(&rowKey, &column, &value) {
		ckv := CqlKeyValue{ rowKey, column, value }
		list = append(list, ckv)
	}

	if err := iter.Close(); err != nil {
		log.Fatalln("cassandra error : ", err)
	}
	return list
}