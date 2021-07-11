from elasticsearch import helpers, Elasticsearch
import csv

es = Elasticsearch()

with open('Date=2004-10-01.csv') as f:
    reader = csv.DictReader(f)
    helpers.bulk(es, reader, index='my-index', doc_type='my-type')