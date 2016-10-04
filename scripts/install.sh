SCRIPT_DIR=$( cd "$(dirname "$0")" ; pwd -P )

mkdir -p $SCRIPT_DIR/../vendor
export GOPATH=$SCRIPT_DIR/../vendor
go get github.com/go-sql-driver/mysql
go get github.com/gocql/gocql
go get github.com/influxdata/influxdb/client
