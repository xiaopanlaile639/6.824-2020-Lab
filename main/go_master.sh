#!/bin/bash

go build -buildmode=plugin ../mrapps/wc.go
rm mr-*
go run mrmaster.go pg-*.txt


