# -*- coding: utf-8 -*-
"""
Transform the WDI data set into DDF data model.

Link for WDI data set: http://databank.worldbank.org/data/download/WDI_csv.zip
"""

import pandas as pd
import numpy as np
import re
import os

from ddf_utils.str import to_concept_id
from ddf_utils.datapackage import dump_json, get_datapackage

# configuration of file path.
source_dir = '../source/'
output_dir = '../../'

data_csv = os.path.join(source_dir, 'WDIData.csv')
country_csv = os.path.join(source_dir, 'WDICountry.csv')
series_csv = os.path.join(source_dir, 'WDISeries.csv')


def extract_concept_discrete(country, series):
    """extract all discrete concepts, base on country and series data."""
    # headers for dataframe and csv file
    header_discrete = ['concept', 'name', 'concept_type']

    # create the dataframe
    concepts_discrete = pd.DataFrame([], columns=header_discrete)
    # all columns in country data and series data are treated as discrete concepts.
    concepts_discrete['name'] = np.concatenate([country.columns, series.columns])
    concepts_discrete['concept'] = concepts_discrete['name'].apply(to_concept_id)

    # remove concepts which are renamed
    concepts_discrete = concepts_discrete[~concepts_discrete.concept.isin(["short_name","indicator_name"])]

    # assign all concepts' type to string, then change the non string concepts
    # to their correct type.
    concepts_discrete['concept_type'] = 'string'

    # adding 'year' and 'country' concept
    concepts_discrete = concepts_discrete.append(
        pd.DataFrame([['country', 'Country', 'entity_domain'],
                      ['year', 'Year', 'time'],
                      ['name', 'Name', 'string'],
                      ], index=[0, 53, 54],
                     columns=concepts_discrete.columns))

    return concepts_discrete


def extract_concept_continuous(country, series):
    """extract all continuous concepts, base on country and series data """

    # all continuous concepts are listed in series data. so no need to create
    # a new data frame.
    concepts_continuous = series.copy()

    # adding some columns for DDF model
    concepts_continuous['concept'] = series['Series Code'].apply(to_concept_id)
    concepts_continuous['concept_type'] = 'measure'

    # rename the columns into lower case alphanumeric and rearrange them
    idxs = np.r_[concepts_continuous.columns[-2:], concepts_continuous.columns[:-2]]
    concepts_continuous = concepts_continuous.loc[:, idxs]
    concepts_continuous.columns = list(map(to_concept_id, concepts_continuous.columns))
    concepts_continuous.rename(index=str, columns={"indicator_name": "name"}, inplace=True)

    return concepts_continuous


def extract_entities_country(country, series):
    """extract all country entities"""

    # just copy the country data from csv.
    entities_country = country.copy()
    entities_country['country'] = entities_country['Country Code'].apply(to_concept_id)

    entities_country.columns = list(map(to_concept_id, entities_country.columns))
    entities_country.rename(index=str, columns={"short_name": "name"}, inplace=True)
    # rearrange the columns
    cols = np.r_[entities_country.columns[-1:], entities_country.columns[:-1]]

    return entities_country.loc[:, cols]


def extract_datapoints_country_year(data):
    """extract all data points by country and year, base on the data csv"""

    res = {}
    # group the data by series.
    gs = data.groupby(by='Indicator Code').groups

    for subject in data['Indicator Code'].unique():
        s = to_concept_id(subject)
        headers_datapoints = ['country', 'year', s]

        data_all = data.ix[gs[subject]].copy()

        data_all['Country Code'] = data_all['Country Code'].apply(to_concept_id)
        data_all = data_all.set_index('Country Code')
        data_all = data_all.T['1960':]  # data begins from 1960

        data_all = data_all.unstack()  # adding back country code as column
        data_all = data_all.reset_index().dropna()  # ... and year column
        data_all.columns = headers_datapoints

        res[s] = data_all

    return res


def remove_cr(df):
    """escape all carrier returns in each cell"""

    def process(s):
        if not pd.isnull(s):
            if isinstance(s, str):
                return s.replace('\n', '\\n')
        return s

    for c in df.columns:
        df[c] = df[c].map(process)


if __name__ == '__main__':

    print('reading source files...')
    data = pd.read_csv(data_csv, encoding='latin', dtype=str,
                       na_values=[''], keep_default_na=False).dropna(how='all', axis=1)
    country = pd.read_csv(country_csv, encoding='latin', dtype=str,
                          na_values=[''], keep_default_na=False).dropna(how='all', axis=1)
    series = pd.read_csv(series_csv, encoding='latin', dtype=str,
                         na_values=[''], keep_default_na=False).dropna(how='all', axis=1)

    print('creating concepts files...')
    concept_continuous = extract_concept_continuous(country, series)
    remove_cr(concept_continuous)
    concept_continuous.to_csv(
        os.path.join(output_dir, 'ddf--concepts--continuous.csv'),
        index=False, encoding='latin')

    concept_discrete = extract_concept_discrete(country, series)
    remove_cr(concept_discrete)
    concept_discrete.to_csv(
        os.path.join(output_dir, 'ddf--concepts--discrete.csv'),
        index=False, encoding='latin')

    print('creating entities files...')
    entities_country = extract_entities_country(country, series)
    remove_cr(entities_country)
    entities_country.to_csv(
        os.path.join(output_dir, 'ddf--entities--country.csv'),
        index=False, encoding='latin')

    print('creating datapoints...')
    datapoints = extract_datapoints_country_year(data)
    for k, v in datapoints.items():
        v[k] = pd.to_numeric(v[k])
        if not v.empty:
            v.to_csv(
                os.path.join(output_dir,
                             'ddf--datapoints--'+k+'--by--country--year.csv'),
                index=False,
                encoding='latin',
                # keep 10 digits. this is to avoid pandas
                # use scientific notation in the datapoints
                # and also keep precision. There are really
                # small/big numbers in this datset.
                float_format='%.10f'
            )

    # datapackage
    dump_json(os.path.join(output_dir, 'datapackage.json'), get_datapackage(output_dir, update=True))
