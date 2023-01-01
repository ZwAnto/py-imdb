
import logging
import os
import re
import subprocess
import tempfile
from io import StringIO
from pathlib import Path
from typing import Union
from urllib.request import urlretrieve

import pandas as pd
from halo import Halo
from tqdm import tqdm

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def my_hook(t):
    """Wraps tqdm instance.
    Don't forget to close() or __exit__()
    the tqdm instance once you're done with it (easiest using `with` syntax).
    Example
    -------
    >>> with tqdm(...) as t:
    ...     reporthook = my_hook(t)
    ...     urllib.urlretrieve(..., reporthook=reporthook)
    """
    last_b = [0]

    def update_to(b=1, bsize=1, tsize=None):
        """
        b  : int, optional
            Number of blocks transferred so far [default: 1].
        bsize  : int, optional
            Size of each block (in tqdm units) [default: 1].
        tsize  : int, optional
            Total size (in tqdm units). If [default: None] remains unchanged.
        """
        if tsize is not None:
            t.total = tsize
        t.update((b - last_b[0]) * bsize)
        last_b[0] = b

    return update_to


class IMDB:

    FILTER = {
        'akas': lambda x: x,
        'basics': lambda x: x
    }

    URL = {
        'akas': "https://datasets.imdbws.com/title.akas.tsv.gz",
        'basics': "https://datasets.imdbws.com/title.basics.tsv.gz"
    }

    FILENAME = {
        'akas': 'title.akas.tsv.gz',
        'basics': 'title.basics.tsv.gz'
    }

    def __init__(
        self,
        refresh: bool = False,
        download_path: Union[str, Path]=Path(tempfile.gettempdir()) / 'pyimdb',
        region: set=set(['FR','GB','US','CA']),
        language: set=set(['fr','en','ca']),
        type: set=set(['movie','tvSeries','tvMiniSeries']),
        chunksize=None
        ):

        self.data = {}
        self.cmd = {}

        self.FILTER['akas']= lambda x: x[x.region.isin(region) & x.language.isin(language)]
        self.FILTER['basics']= lambda x: x[x.titleType.isin(type)]

        if not isinstance(download_path, Path):
            download_path = Path(download_path)
        if not os.path.isdir(download_path):
            os.makedirs(download_path, exist_ok=True)

        self.filename = {k:download_path / v for k,v in self.FILENAME.items()}

        self.rows = {k: self.__count_rows(f.with_suffix('')) for k,f in self.filename.items()}

        if refresh or not all([os.path.exists(i.with_suffix('')) for i in self.filename.values()]):
            self.__download()

        if chunksize is None:
            self.__load()
        else:
            self.__load_chunk(chunksize)

    def __count_rows(self,file):
        a = subprocess.Popen(f'wc -l {file}', stdout=subprocess.PIPE, shell=True)
        return int(a.communicate()[0].decode('utf-8').split()[0]) - 1

    def __download(self):

        for name, url in self.URL.items():
            with tqdm(unit = 'B', unit_scale = True, unit_divisor = 1024, miniters = 1, desc = name) as t:
                urlretrieve(url, self.filename[name], my_hook(t))

            _ = subprocess.call(f"gunzip -f {self.filename[name]}".split())

    def __load(self):

        for name in self.filename.keys():

            spinner = Halo(text=f'Loading data from {self.filename[name].with_suffix("")}', spinner='dots')
            spinner.start()

            try:
                self.data[name] = self.FILTER[name](pd.read_csv(self.filename[name].with_suffix(""), sep="\t", dtype=str))
                spinner.succeed()
            except:
                spinner.fail()
        
    def __chunk_generator(self, name, chunksize):
        for chunk in pd.read_csv(self.filename[name].with_suffix(""), chunksize=chunksize, sep="\t", dtype=str):
            yield self.FILTER[name](chunk)

    def __load_chunk(self, chunksize):

        for name in self.filename.keys():
            self.data[name] = self.__chunk_generator(name, chunksize)

    def clear(self):

        for file in self.filename.values():

            spinner = Halo(text=f'Removing {file}', spinner='dots')
            spinner.start()

            try:
                os.unlink(file)
                spinner.succeed()
            except:
                spinner.fail()
