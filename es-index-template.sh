#!/bin/bash
path=`dirname "$0"`
curl -XPUT \
	-H 'Content-Type: application/json' \
	"${ES_HOST:-localhost}:${ES_PORT:-9200}/_template/statsd-template" \
	-d @$path/template.json
