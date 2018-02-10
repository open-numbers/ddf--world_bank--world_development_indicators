# -*- coding: utf-8 -*-

from pathlib import Path
from zipfile import ZipFile
from ddf_utils.factory import worldbank as wb


source_dir = '../source/'

def update():
    print('downloading source data...')
    wb.bulk_download('WDI', source_dir)
    print('extracting...')
    f = ZipFile(Path(source_dir, 'WDI_csv.zip'))
    f.extractall(source_dir)


if __name__ == '__main__':
    update()
