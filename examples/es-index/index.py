
from pathlib import Path

import numpy as np
import requests
from elasticsearch import Elasticsearch, helpers
from pyimdb.imdb import IMDB
from tqdm import tqdm

import os

ES_HOST = os.environ.get('ES_HOST')
ES_PORT = os.environ.get('ES_PORT')

es = Elasticsearch(hosts=f'http://{ES_HOST}:{ES_PORT}/')
es.info()

for index in Path('es_indices').glob('*.json'):
  _ = requests.delete(f"http://{ES_HOST}:{ES_PORT}/{index.stem}")
  r = requests.put(f"http://{ES_HOST}:{ES_PORT}/{index.stem}", open(index).read(), headers= {'Content-Type': 'application/json'})
  assert r.status_code == 200

imdb = IMDB()

basics = imdb.data['basics'][["tconst", "titleType", "originalTitle", "primaryTitle", "startYear"]] \
  .query("titleType == 'movie' or titleType == 'tvSeries' or titleType=='tvMiniSeries'") \
  .melt(['tconst', 'titleType', 'startYear'], var_name='source', value_name='title')

basics['_index'] = 'imdb-movie-basics'
basics.loc[basics.titleType.isin(['tvSeries', 'tvMiniSeries']), '_index'] = 'imdb-tv-basics'

akas = imdb.data['akas'] \
    .merge(basics[["tconst", "titleType", "startYear", "_index"]], left_on='titleId', right_on='tconst', how='inner')
    #.query('isOriginalTitle != "1"') \

akas['_index'] = akas['_index'].str.replace('basics', 'akas')
akas['source'] = 'akas'
akas['imdbId'] = akas['titleId']
akas['_id'] = akas['titleId'] + "-" + akas['source'] + "-" + akas['ordering']


basics['source'] = basics['source'].str.replace('Title', '')
basics['imdbId'] = basics['tconst']
basics['_id'] =  basics['tconst'] + "-" + basics['source']


akas = akas[['_index', '_id', 'source', 'imdbId', 'title', 'titleType', 'startYear', 'region', 'language']]
basics = basics[['_index', '_id', 'source', 'imdbId',  'title', 'titleType', 'startYear']]


akas.rename(columns={
    "startYear": "year",
    "titleType": "type"
}, inplace = True)
basics.rename(columns={
    "startYear": "year",
    "titleType": "type"
}, inplace = True)

for db in [basics, akas]:

  data_chunks = np.array_split(db, 100000)

  for chunk in tqdm(data_chunks):
      actions = chunk.to_dict('records')
      _ = helpers.bulk(es, actions)
