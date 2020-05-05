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
domain_xls = os.path.join(source_dir, 'wb_economy_entity_domain.xlsx')

CONCEPTS = list()
DOMAINS = list()


def extrace_economy_entities(domains: pd.DataFrame, groups: pd.DataFrame):
    """create domains/sets for economics"""
    all_entities = list()
    set_membership = dict()

    for _, row in domains.iterrows():
        name = row['name']
        sets = row['set membership']
        sets_list = list(map(str.strip, sets.split(',')))
        id = to_concept_id(row['economy'])
        set_membership[row['economy']] = sets_list
        all_entities.append(
            Entity(id=id,
                   domain='economy',
                   sets=sets_list,
                   props={'name': name}))

    grouped = groups.groupby(by=['CountryCode'])
    for eco, idx in grouped.groups.items():
        df = groups.iloc[idx]
        eco_groups = df['GroupCode'].values.tolist()
        eco_id = to_concept_id(eco)
        eco_name = df['CountryName'].unique()
        if len(eco_name) > 1:
            print(f'Warning: economy {eco} has multiple names: {eco_name}')
        props = {'name': eco_name[0]}
        for g in eco_groups:
            try:
                sets = set_membership[g]
            except KeyError:
                print("warning: group not found: {g}, please add it to the wb_economy_entity_domain.xlsx file.")
                raise
            for s in sets:
                props[s] = to_concept_id(g)
        all_entities.append(
            Entity(id=eco_id, domain='economy', sets=['country'], props=props))

    return all_entities


def main():
    print("reading source files...")
    # data = pd.read_csv(data_csv,
    #                    encoding='latin',
    #                    dtype=str,
    #                    na_values=[''],
    #                    keep_default_na=False).dropna(how='all', axis=1)
    # country = pd.read_csv(country_csv,
    #                       encoding='latin',
    #                       dtype=str,
    #                       na_values=[''],
    #                       keep_default_na=False).dropna(how='all', axis=1)
    # series = pd.read_csv(series_csv,
    #                      encoding='latin',
    #                      dtype=str,
    #                      na_values=[''],
    #                      keep_default_na=False).dropna(how='all', axis=1)

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

    domains = pd.read_excel(domain_xls)
    all_entities = extrace_economy_entities(domains, groups)
    eco_domain = EntityDomain(id='economy', entities=all_entities, props={'name': 'Economy'})
    for eset in eco_domain.entity_sets:
        df = pd.DataFrame.from_records([v.to_dict() for v in eco_domain.get_entity_set(eset)])
        df.to_csv(f'../../ddf--entities--economy--{eset}.csv', index=False)


if __name__ == '__main__':
    main()
    print('Done.')
