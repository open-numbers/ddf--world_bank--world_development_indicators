# -*- coding: utf-8 -*-
"""
Transform the WDI data set into DDF data model.

Link for WDI data set: http://databank.worldbank.org/data/download/WDI_csv.zip
"""

import pandas as pd
import numpy as np
import os

from ddf_utils.str import to_concept_id
from ddf_utils.io import dump_json
from ddf_utils.package import get_datapackage
from ddf_utils.model.ddf import DataPoint, Concept, EntityDomain, Entity

# configuration of file path.
source_dir = '../source/'
output_dir = '../../'

data_csv = os.path.join(source_dir, 'WDIData.csv')
country_csv = os.path.join(source_dir, 'WDICountry.csv')
series_csv = os.path.join(source_dir, 'WDISeries.csv')
groups_xls = os.path.join(source_dir, 'CLASS.xls')

CONCEPTS = list()
DOMAINS = list()


def extract_all_entities(econs: pd.DataFrame, groups: pd.DataFrame):
    """create domains/sets for economics"""

    regions = econs[pd.isnull(econs['Region'])][['Economy', 'Code']]
    bare_regions = econs['Region'].unique()
    bare_income_groups = econs['Income group'].unique()
    bare_lending_cats = econs['Lending category'].unique()

    def infer_domain_set(name):
        if name in bare_regions:
            domain = 'region'
            eset = []
        elif '(excluding high income)' in name:
            domain = 'region'
            eset = 'excluding_high_income'
        elif ' (IDA & IBRD)' in name:
            domain = 'region'
            eset = 'ida_and_ibrd'
        elif 'demographic dividend' in name:
            domain = 'demographic_dividend_state'
            eset = []
        elif name in bare_income_groups:
            domain = 'income_group'
            eset = []
        elif name in bare_lending_cats:
            domain = 'lending_category'
            eset = []
        elif name == 'World':
            domain = 'region'
            eset = 'global'
        elif 'small states' in name:
            domain = 'small_state'
            eset = []
        else:
            domain = 'other_group'
            eset = []
        return domain, eset

    all_entities = list()

    for _, row in regions.iterrows():
        name = row['Economy']
        code = row['Code']
        eid = to_concept_id(code)
        domain, eset = infer_domain_set(name)
        if eset:
            eset = [eset]
        all_entities.append(Entity(id=eid, domain=domain, sets=eset, props={'name': name}))

    countries = econs[~pd.isnull(econs['Region'])][['Economy', 'Code']]
    groups = groups.groupby('CountryCode')
    for _, row in countries.iterrows():
        name = row['Economy']
        code = row['Code']
        eid = to_concept_id(code)
        domain = 'country'
        props = dict()
        for _, gr in groups.get_group(code).iterrows():
            other_domain, other_set = infer_domain_set(gr['GroupName'])
            other_group_code = to_concept_id(gr['GroupCode'])
            if other_set:
                props[other_set] = other_group_code
            else:
                props[other_domain] = other_group_code
        all_entities.append(Entity(id=eid, domain=domain, sets=[], props=props))

    return all_entities


def create_domain(all_entities, domain_name, domain_id=None):

    if not domain_id:
        domain_id = to_concept_id(domain_name)

    domain = EntityDomain(
        id=domain_id,
        entities=[x for x in all_entities if x.domain == domain_id],
        props={'name': domain_name})

    DOMAINS.append(domain)


def main():
    print("reading source files...")
    data = pd.read_csv(data_csv,
                       encoding='latin',
                       dtype=str,
                       na_values=[''],
                       keep_default_na=False).dropna(how='all', axis=1)
    country = pd.read_csv(country_csv,
                          encoding='latin',
                          dtype=str,
                          na_values=[''],
                          keep_default_na=False).dropna(how='all', axis=1)
    series = pd.read_csv(series_csv,
                         encoding='latin',
                         dtype=str,
                         na_values=[''],
                         keep_default_na=False).dropna(how='all', axis=1)
    econs = pd.read_excel(
        groups_xls,
        sheet_name='List of economies',
        encoding='latin',
        skiprows=4,
        na_values=['..', '', 'x'],
        keep_default_na=False).dropna(how='all').dropna(subset=['Code'])
    groups = pd.read_excel(groups_xls,
                           sheet_name='Groups',
                           encoding='latin',
                           na_values=[''],
                           keep_default_na=False).dropna(how='all')

    # load entities and create domains
    all_entities = extract_all_entities(econs=econs, groups=groups)
    create_domain(all_entities, "Country")
    create_domain(all_entities, "Region")
    create_domain(all_entities, "Demographic Dividend State")
    create_domain(all_entities, "Small State")
    create_domain(all_entities, "Lending Category")
    create_domain(all_entities, "Other Group")

    for domain in DOMAINS:
        domain_df = pd.DataFrame.from_records(domain.to_dict())
        # print(domain.id)
        # print(domain_df.head())
        domain_df.to_csv(f'../../ddf--entities--{domain.id}.csv', index=False)

if __name__ == '__main__':
    main()
    print('Done.')
