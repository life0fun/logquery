# Elastic Logquery

Query logs in ElasticSearch store using elastic RESTful APIs.
Process log and perform statistic computing and ploting the data using R-like Incanter library.

## Installation

lein deps
lein compile
lein run

## Usage

lein run stats index-name

## Options

Configurations for elasticstore host and port.

## ElasticSearch Store API

First, get familar with elastic search API.
	 
	http://www.slideshare.net/clintongormley/terms-of-endearment-the-elasticsearch-query-dsl-explained

You can find out what query string been sent to elasticsearch by looking into Kibana network section.

kibana curls to elastic on browser tools, jsconsole, network, then issue a search, check kibana section.

	; "curl -XGET http://localhost:9200/logstash-2013.05.23/_search?pretty -d ' 
	; {"size": 100, 
	;  "query": {
	;     "filtered":{
	;       "query":{ 
	;         "query_string":{
	;           "default_operator": "OR", default_field": "@message",
	;           "query": "@message:Stats AND @type:finder_core_application"}}, <- close of query
	;       "filter": {
	;         "range": {
	;           "@timestamp": {
	;             "from": "2013-05-22T16:10:48Z", "to": "2013-05-23T02:10:48Z"}}}}},
	;   "from": 0,
	;   "sort": {
	;     "@timestamp":{
	;       "order": "desc"}},


## Incanter

We using Incanter for basic statistic computing and ploting data.



