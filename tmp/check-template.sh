#!/usr/bin/env bash

curl -s -X GET http://localhost:8083/connectors \
	| jq -M '.[]' \
	| sed -e 's/"//g' \
	| sort \
	| while read CONNECTOR; do
	echo -n "Checking $CONNECTOR: "
	curl -s -X GET http://localhost:8083/connectors/$CONNECTOR/status | jq -M '.tasks[].state'
done
