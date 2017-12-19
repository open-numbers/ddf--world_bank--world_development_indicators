# -*- coding: utf-8 -*-
"""
Transform the WDI data set into DDF data model.

Link for WDI data set: http://databank.worldbank.org/data/download/WDI_csv.zip
"""

import pandas as pd
import numpy as np
import re
import os

# configuration of file path.
source_dir = '../source/WDI_csv/'
output_dir = '../../'

data_csv = os.path.join(source_dir, 'WDI_Data.csv')
country_csv = os.path.join(source_dir, 'WDI_Country.csv')
series_csv = os.path.join(source_dir, 'WDI_Series.csv')


# functions for creating DDF files.
def to_concept_id(s):
    '''convert a string to lowercase alphanumeric + underscore id for concepts'''
    return re.sub(r'[/ -\.]+', '_', s).lower()


def extract_concept_discrete(country, series):
    """extract all discrete concepts, base on country and series data."""
    # headers for dataframe and csv file
    header_discrete = ['concept', 'name', 'concept_type']

    # create the dataframe
    concepts_discrete = pd.DataFrame([], columns=header_discrete)
    # all columns in country data and series data are treated as discrete concepts.
    concepts_discrete['name'] = np.concatenate([country.columns, series.columns])
    concepts_discrete['concept'] = concepts_discrete['name'].apply(to_concept_id)

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

    return concepts_continuous


def extract_entities_country(country, series):
    """extract all country entities"""

    # just copy the country data from csv.
    entities_country = country.copy()
    entities_country['country'] = entities_country['Country Code'].apply(to_concept_id)

    entities_country.columns = list(map(to_concept_id, entities_country.columns))
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


if __name__ == '__main__':
    print('reading source files...')
    data = pd.read_csv(data_csv)
    country = pd.read_csv(country_csv, encoding='latin', dtype=str)
    series = pd.read_csv(series_csv, encoding='latin', dtype=str)

    print('creating concepts files...')
    concept_continuous = extract_concept_continuous(country, series)
    concept_continuous.to_csv(
        os.path.join(output_dir, 'ddf--concepts--continuous.csv'),
        index=False, encoding='utf8')

    concept_discrete = extract_concept_discrete(country, series)
    concept_discrete.to_csv(
        os.path.join(output_dir, 'ddf--concepts--discrete.csv'),
        index=False, encoding='utf8')

    print('creating entities files...')
    entities_country = extract_entities_country(country, series)
    entities_country.to_csv(
        os.path.join(output_dir, 'ddf--entities--country.csv'),
        index=False, encoding='utf8')

    print('creating datapoints...')
    datapoints = extract_datapoints_country_year(data)
    for k, v in datapoints.items():
        v[k] = pd.to_numeric(v[k])
        v.to_csv(
            os.path.join(output_dir,
                         'ddf--datapoints--'+k+'--by--country--year.csv'),
            index=False,
            encoding='utf8',
            # keep 10 digits. this is to avoid pandas
            # use scientific notation in the datapoints
            # and also keep precision. There are really
            # small/big numbers in this datset.
            float_format='%.10f'
        )
