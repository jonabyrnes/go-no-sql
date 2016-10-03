mkdir -p vendor
export GOPATH=`pwd`/vendor
go get github.com/go-sql-driver/mysql
go get github.com/gocql/gocql
