# -*- coding: utf-8 -*-

import os

from zipfile import ZipFile
# from ddf_utils.factory import WorldBankLoader
from ddf_utils.factory.common import download


source_dir = '../source/'
# classification files
# see https://datahelpdesk.worldbank.org/knowledgebase/articles/906519-world-bank-country-and-lending-groups
zip_file = "http://databank.worldbank.org/data/download/WDI_csv.zip"
url_class_xls = 'http://databank.worldbank.org/data/download/site-content/CLASS.xlsx'
url_oghist_xls = 'http://databank.worldbank.org/data/download/site-content/OGHIST.xlsx'


def update():
    # wb = WorldBankLoader()
    # print('downloading source data...')
    # wb.bulk_download('WDI', source_dir, timeout=60)
    download(zip_file, '../source/WDI_csv.zip')
    print('extracting...')
    f = ZipFile(os.path.join(source_dir, 'WDI_csv.zip'))
    f.extractall(source_dir)
    print('downloading CLASS.xls...')
    download(url_class_xls, os.path.join(source_dir, 'CLASS.xlsx'))
    print('downloading OGHIST.xls...')
    download(url_oghist_xls, os.path.join(source_dir, 'OGHIST.xlsx'))


if __name__ == '__main__':
    update()
