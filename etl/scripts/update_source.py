# -*- coding: utf-8 -*-

import os

from zipfile import ZipFile
# from ddf_utils.factory import WorldBankLoader
# from ddf_utils.factory.common import download
import requests


def download(url, outpath):
    response = requests.get(url, allow_redirects=True)
    open(outpath, 'wb').write(response.content)


source_dir = '../source/'
# classification files
# see https://datahelpdesk.worldbank.org/knowledgebase/articles/906519-world-bank-country-and-lending-groups
zip_file = "https://databank.worldbank.org/data/download/WDI_CSV.zip"
url_class_xls = 'https://datacatalogapi.worldbank.org/ddhxext/ResourceDownload?resource_unique_id=DR0090755'
url_oghist_xls = 'https://datacatalogapi.worldbank.org/ddhxext/ResourceDownload?resource_unique_id=DR0090754'


def update():
    print('downloading source data...')
    download(zip_file, '../source/WDI_csv.zip')
    print('extracting...')
    f = ZipFile(os.path.join(source_dir, 'WDI_csv.zip'))
    f.extractall(source_dir)
    print('downloading CLASS.xlsx...')
    download(url_class_xls, os.path.join(source_dir, 'CLASS.xlsx'))
    print('downloading OGHIST.xlsx...')
    download(url_oghist_xls, os.path.join(source_dir, 'OGHIST.xlsx'))


if __name__ == '__main__':
    update()
