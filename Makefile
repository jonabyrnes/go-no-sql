export GOPATH:=$(CURDIR)/vendor

csv:
	go run ./src/go-csv-to-influx.go
