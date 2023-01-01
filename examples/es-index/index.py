
import os
from pathlib import Path

import numpy as np
import pandas as pd
import requests
from elasticsearch import Elasticsearch, helpers
from tqdm import tqdm
import math
from pyimdb.imdb import IMDB

ES_HOST = os.environ.get('ES_HOST')
ES_PORT = os.environ.get('ES_PORT')

es = Elasticsearch(hosts=f'http://{ES_HOST}:{ES_PORT}/')
es.info()

for index in Path('es_indices').glob('*.json'):
  _ = requests.delete(f"http://{ES_HOST}:{ES_PORT}/{index.stem}")
  r = requests.put(f"http://{ES_HOST}:{ES_PORT}/{index.stem}", open(index).read(), headers= {'Content-Type': 'application/json'})
  assert r.status_code == 200


def process_basics(df):

  basics = df[["tconst", "titleType", "originalTitle", "primaryTitle", "startYear"]] \
    .melt(['tconst', 'titleType', 'startYear'], var_name='source', value_name='title')

  basics['_index'] = 'imdb-movie-basics'
  basics.loc[basics.titleType.isin(['tvSeries', 'tvMiniSeries']), '_index'] = 'imdb-tv-basics'
  
  basics['source'] = basics['source'].str.replace('Title', '')
  basics['imdbId'] = basics['tconst']
  basics['_id'] =  basics['tconst'] + "-" + basics['source']
    
  basics = basics[['_index', '_id', 'source', 'imdbId',  'title', 'titleType', 'startYear']]

  basics.rename(columns={
    "startYear": "year",
    "titleType": "type"
  }, inplace = True)

  return basics

def process_akas(df, basics):

  akas = df[["titleId", "ordering", "title", "region", "language"]] \
      .merge(basics[["imdbId", "type", "year", "_index"]], left_on='titleId', right_on='imdbId', how='inner')

  akas['_index'] = akas['_index'].str.replace('basics', 'akas')
  akas['source'] = 'akas'
  akas['imdbId'] = akas['titleId']
  akas['_id'] = akas['titleId'] + "-" + akas['source'] + "-" + akas['ordering']

  akas = akas[['_index', '_id', 'source', 'imdbId', 'title', 'type', 'year', 'region', 'language']]

  return akas

chunksize=10000

imdb = IMDB(chunksize=chunksize)

_basics = None
for rows in tqdm(imdb.data['basics'], total=math.ceil(imdb.rows['basics']/chunksize)):
  basics = process_basics(rows)

  if basics.empty:
    continue

  if _basics is None:
    _basics = basics[["imdbId", "type", "year", "_index"]].drop_duplicates()
  else:
    _basics = pd.concat([_basics, basics[["imdbId", "type", "year", "_index"]]]).drop_duplicates()

  actions = basics.to_dict('records')
  _ = helpers.bulk(es, actions)


for rows in tqdm(imdb.data['akas'], total=math.ceil(imdb.rows['akas']/chunksize)):
  akas = process_akas(rows, _basics)

  if akas.empty:
    continue

  actions = akas.to_dict('records')
  _ = helpers.bulk(es, actions)
