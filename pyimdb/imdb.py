
import logging
import os
import subprocess
import tempfile
from io import StringIO
from pathlib import Path
from urllib.request import urlretrieve
from typing import Union
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

    CMD = {
        'akas': "zgrep -P '^[^\t]+\t[^\t]+\t[^\t]+\t(\\N|{})\t(\\N|{})|titleId' {{}}",
        'basics': "zgrep -P '^tt[0-9]+\t({})\t|tconst' {{}}",
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
        region: list=['FR','GB','US','CA'],
        language: list=['fr','en','ca'],
        type: list=['movie','tvSeries','tvMiniSeries']
        ):

        self.data = {}
        self.cmd = {}

        self.cmd['akas'] = self.CMD['akas'].format('|'.join(region), '|'.join(language))
        self.cmd['basics'] = self.CMD['basics'].format('|'.join(type))


        if not isinstance(download_path, Path):
            download_path = Path(download_path)
        if not os.path.isdir(download_path):
            os.makedirs(download_path, exist_ok=True)

        self.filename = {k:download_path / v for k,v in self.FILENAME.items()}

        if refresh or not all([os.path.exists(i) for i in self.filename.values()]):
            self.__download()

        self.__load()

    def __download(self):

        for name, url in self.URL.items():
            with tqdm(unit = 'B', unit_scale = True, unit_divisor = 1024, miniters = 1, desc = name) as t:
                urlretrieve(url, self.filename[name], my_hook(t))

    def __load(self):

        for name, cmd in self.cmd.items():

            spinner = Halo(text=f'Loading data from {self.filename[name]}', spinner='dots')
            spinner.start()

            try:
                a = subprocess.Popen(cmd.format(self.filename[name]), stdout=subprocess.PIPE, shell=True)
                b = StringIO(a.communicate()[0].decode('utf-8'))

                self.data[name] = pd.read_csv(b, sep="\t", dtype=str)

                spinner.succeed()
            except:
                spinner.fail()

        self.data['akas'] = self.data['akas'][self.data['akas'].titleId.isin(self.data['basics'].tconst)]
        
    def clear(self):

        for file in self.filename.values():

            spinner = Halo(text=f'Removing {file}', spinner='dots')
            spinner.start()

            try:
                os.unlink(file)
                spinner.succeed()
            except:
                spinner.fail()
