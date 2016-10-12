package mysql

import (
	"log"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)

type SqlKeyValue struct {
	Key string
	Value string
}

func GetDatabase(url string) *sql.DB {
	db, err := sql.Open("mysql", url)
	if err != nil {
		log.Fatalln("mysql error : ", err)
	}
	return db
}


func SqlQuery(db *sql.DB, q string) *sql.Rows {
	rows, err := db.Query(q)
	if err != nil {
		log.Fatalln("error executing query : ", err)
	}
	return rows
}

func GetSqlKeyValues(db *sql.DB, sql string) []SqlKeyValue {

	// select the groups by top level network group
	rows := SqlQuery( db, sql )
	defer rows.Close()

	list := []SqlKeyValue{}
	for rows.Next() {
		pair := SqlKeyValue{}
		if err := rows.Scan( &pair.Key,  &pair.Value ); err != nil {
			log.Fatalln("error loading sql row : ", err)
		}
		list = append(list, pair)
	}

	if err := rows.Err(); err != nil {
		log.Fatalln("mysql error : ", err)
	}
	return list
}
