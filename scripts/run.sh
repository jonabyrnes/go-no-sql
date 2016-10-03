SCRIPT_DIR=$( cd "$(dirname "$0")" ; pwd -P )

export GOPATH=$SCRIPT_DIR/../vendor
go run $SCRIPT_DIR/../go-no-sql.go