# -*- coding: utf-8 -*-

import os
import time

from zipfile import ZipFile
# from ddf_utils.factory import WorldBankLoader
# from ddf_utils.factory.common import download
import requests


def download(url, outpath, max_retries=10):
    for _ in range(max_retries):
        response = requests.get(url, allow_redirects=True)
        if response.status_code == 429:
            # the datacatalog api is rate limited, wait and retry
            wait = int(response.headers.get('Retry-After', 30)) + 1
            print(f'rate limited, retrying in {wait} seconds...')
            time.sleep(wait)
            continue
        response.raise_for_status()
        with open(outpath, 'wb') as f:
            f.write(response.content)
        return
    raise RuntimeError(f'failed to download {url}: too many retries')


source_dir = '../source/'
# classification files
# see https://datahelpdesk.worldbank.org/knowledgebase/articles/906519-world-bank-country-and-lending-groups
zip_file = "https://databank.worldbank.org/data/download/WDI_CSV.zip"
url_class_xls = 'https://ddh-openapi.worldbank.org/resources/DR0095333/download'
url_oghist_xls = 'https://ddh-openapi.worldbank.org/resources/DR0095334/download'


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
