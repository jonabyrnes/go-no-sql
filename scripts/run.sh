SCRIPT_DIR=$( cd "$(dirname "$0")" ; pwd -P )

export GOPATH=$SCRIPT_DIR/..:$SCRIPT_DIR/../vendor
go run $SCRIPT_DIR/../src/go-metric-loader.go