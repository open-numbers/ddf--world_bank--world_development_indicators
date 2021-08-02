# -*- coding: utf-8 -*-
"""
Transform the WDI data set into DDF data model.

Link for WDI data set: http://databank.worldbank.org/data/download/WDI_csv.zip
"""

import pandas as pd
import os

from ddf_utils.str import to_concept_id
from ddf_utils.io import dump_json
from ddf_utils.package import get_datapackage
from ddf_utils.model.ddf import Concept, EntityDomain, Entity

from income_group_hist import load_and_pre_process, create_hist_income_group_datapoints

# configuration of file path.
source_dir = '../source/'
output_dir = '../../'

data_csv = os.path.join(source_dir, 'WDIData.csv')
country_csv = os.path.join(source_dir, 'WDICountry.csv')
series_csv = os.path.join(source_dir, 'WDISeries.csv')
groups_xls = os.path.join(source_dir, 'CLASS.xls')
domain_xls = os.path.join(source_dir, 'wb_economy_entity_domain.xlsx')
oghist_file = os.path.join(source_dir, 'OGHIST.xls')

country_mask_col = ['Region',
                    'Income Group',
                    'Lending category',
                    'Other groups']  # we will read groupings from class.xls, so drop these cols from wdicountry.csv


# function to generate all economy entities with entity sets properties
# we will use WDICountry.csv from the bulk downloaded WDI data
# and the CLASS.xls from WDI country classification
# But CLASS.xls not always having all entities listed in WDICountry.csv
# so we need to check both file to get all entities
def extract_economy_entities(countries: pd.DataFrame, domains: pd.DataFrame, groups: pd.DataFrame):
    """create domains/sets for economics"""
    all_entities = dict()
    set_membership = dict()

    for _, row in domains.iterrows():
        name = row['name']
        sets = row['set membership']
        sets_list = list(map(str.strip, sets.split(',')))
        eco_id = to_concept_id(row['economy'])
        set_membership[row['economy']] = sets_list
        all_entities[eco_id] = Entity(id=eco_id,
                                      domain='economy',
                                      sets=sets_list,
                                      props={'name': name})

    grouped = groups.groupby(by=['CountryCode'])
    for eco, df in grouped:
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
                print("warning: group not found: {g}, please add it to the "
                      "wb_economy_entity_domain.xlsx file.")
                raise
            for s in sets:
                group_concept = to_concept_id(g)
                # we do not allow multiple membership
                if s in props:
                    raise ValueError(
                        f'{eco_name} belongs to 2 groups '
                        '({props[s]}, {group_concept}) in same entity_set {s}')
                props[s] = group_concept

        all_entities[eco_id] = Entity(id=eco_id, domain='economy', sets=['country'], props=props)

    for code, row in countries.iterrows():
        name = row['name']
        eco_id = code.lower()
        if eco_id not in all_entities:  # in this case, it's not in any of entity sets
            print(f"found {name} which is not in any of entity sets")
            props = row.to_dict()
            all_entities[eco_id] = Entity(id=eco_id, domain='economy', sets=[], props=props)
        else:
            props = row.to_dict()
            ent = all_entities[eco_id]
            all_entities[eco_id] = Entity(id=eco_id, domain='economy', sets=ent.sets, props=props)

    return list(all_entities.values())


def remove_cr(df):
    """escape all carrier returns in each cell"""

    def process(s):
        if not pd.isnull(s):
            if isinstance(s, str):
                return s.replace('\n', '\\n')
        return s

    for c in df.columns:
        df[c] = df[c].map(process)


def extract_concept_series(series):
    """extract all concepts from series metadata"""

    df = series.copy()
    df['concept'] = series['Series Code'].apply(to_concept_id)
    df = df.set_index('concept')

    # use 'Indicator Name' column as name to indicator concepts
    df = df.rename(columns={'Indicator Name': 'Name'})

    remove_cr(df)

    # first emit all discrete concepts
    for column in df.columns:
        yield Concept(id=to_concept_id(column),
                      concept_type='string',
                      props={'name': column})

    # then emit all continuous concepts
    df.columns = df.columns.map(to_concept_id)
    for concept, row in df.iterrows():
        props = row.to_dict()
        yield Concept(id=concept, concept_type='measure', props=props)


def extract_concept_entities(domain):

    # first yield the domain concept
    yield Concept(id=domain.id,
                  concept_type='entity_domain',
                  props={'name': domain.props['name']})

    # then the entity sets:
    for eset in domain.entity_sets:
        name = ' '.join(eset.split('_')).title()
        yield Concept(id=eset,
                      concept_type='entity_set',
                      props={
                          'name': name,
                          'domain': domain.id
                      })


def extract_datapoints_country_year(data):
    """extract all data points by country and year, base on the data csv"""

    res = {}

    # usually data begins from 1960, but we should ensure that.
    print("Data should begins from 1960. Here are columns we won't include in "
          "datapoints. Please check if they are correct:")
    idx = list(data.iloc[0].index).index('1960')
    print(data.iloc[0].index[:idx])

    # group the data by series.
    grouped = data.groupby(by='Indicator Code')

    for indicator, df in grouped:
        concept = to_concept_id(indicator)
        headers_datapoints = ['economy', 'year', concept]

        df_group = df.copy()
        df_group['Country Code'] = df_group['Country Code'].apply(to_concept_id)
        df_group = df_group.set_index('Country Code')
        df_group = df_group.T['1960':]  # data begins from 1960
        df_group = df_group.unstack()  # adding back country code as column
        df_group = df_group.reset_index().dropna()  # ... and year column
        df_group.columns = headers_datapoints

        res[concept] = df_group

    return res


def main():
    print("reading source files...")
    data = pd.read_csv(data_csv,
                       dtype=str,
                       na_values=[''],
                       keep_default_na=False).dropna(how='all', axis=1)
    series = pd.read_csv(series_csv,
                         dtype=str,
                         na_values=[''],
                         keep_default_na=False).dropna(how='all', axis=1)

    groups = pd.read_excel(groups_xls,
                           sheet_name='Groups',
                           na_values=[''],
                           keep_default_na=False).dropna(how='all')

    # domain
    print('creating economy domain...')
    domains = pd.read_excel(domain_xls)
    countries = (pd.read_csv(country_csv, keep_default_na=False, na_values=[''], dtype=str)
                 .dropna(how='all', axis=1)
                 .drop(country_mask_col, axis=1)
                 .rename(columns={'Table Name': 'Name'})
                 .set_index('Country Code'))
    countries_cols = countries.columns
    countries.columns = countries.columns.map(to_concept_id)
    all_entities = extract_economy_entities(countries, domains, groups)

    # domain: add more entity from income group history
    income_hist_table = load_and_pre_process(oghist_file)
    income_economy = income_hist_table[
        'economy_name']  # the index is economy IDs.
    existing = [e.id for e in all_entities]
    missing = [i for i in income_economy.index if i not in existing]
    for i in missing:
        name = income_economy[i]
        all_entities.append(
            Entity(id=i,
                   domain='economy',
                   sets=['country'],
                   props=dict(name=name)))

    eco_domain = EntityDomain(id='economy',
                              entities=all_entities,
                              props={'name': 'Economy'})
    for eset in eco_domain.entity_sets:
        df = pd.DataFrame.from_records(eco_domain.to_dict(eset=eset))
        # some entities belong to multiple entity_sets, but we don't need to
        # include other is-- headers than the one we are proceeding.
        # TODO: maybe change the EntityDomain.to_dict method
        for col in df.columns:
            if col.startswith('is--') and col != f'is--{eset}':
                df = df.drop(col, axis=1)
        df.to_csv(f'../../ddf--entities--economy--{eset}.csv', index=False)
    df_nosets = pd.DataFrame.from_records(eco_domain.to_dict(eset=[]))
    df_nosets.to_csv('../../ddf--entities--economy.csv', index=False)

    # datapoints
    print('creating datapoints...')
    datapoints = extract_datapoints_country_year(data)
    datapoints_output_dir = os.path.join(output_dir, 'datapoints')
    os.makedirs(datapoints_output_dir, exist_ok=True)
    for k, v in datapoints.items():
        v[k] = pd.to_numeric(v[k])
        if not v.empty:
            v.to_csv(
                os.path.join(
                    datapoints_output_dir,
                    'ddf--datapoints--' + k + '--by--economy--year.csv'),
                index=False,
                # keep 10 digits. this is to avoid pandas
                # use scientific notation in the datapoints
                # and also keep precision. There are really
                # small/big numbers in this datset.
                float_format='%.10f')

    # income group history datapoints
    income_hist_dp = create_hist_income_group_datapoints(income_hist_table)
    for lvl, df in income_hist_dp.items():
        df.to_csv(os.path.join(output_dir,
                       f'ddf--datapoints--{lvl}--by--economy--year.csv'),
                  index=False)

    # concepts
    print('creating concepts files...')
    concepts = list()
    for c in extract_concept_series(series):
        concepts.append(c)

    concept_continuous = [
        c.to_dict() for c in concepts if c.concept_type == 'measure'
    ]
    concept_continuous_df = pd.DataFrame.from_records(concept_continuous)
    concept_continuous_df.to_csv(os.path.join(output_dir,
                                              'ddf--concepts--continuous.csv'),
                                 index=False)

    concept_discrete = [c for c in concepts if c.concept_type != 'measure']
    for c in extract_concept_entities(eco_domain):
        concept_discrete.append(c)
    # hard code the `year` and `domain` concept
    concept_discrete.append(
        Concept(id='year', concept_type='time', props=dict(name='Year')))
    concept_discrete.append(
        Concept(id='domain', concept_type='string', props=dict(name='Domain')))

    concept_discrete = [c.to_dict() for c in concept_discrete]
    concept_discrete_df = pd.DataFrame.from_records(concept_discrete)
    concept_discrete_df = concept_discrete_df.sort_values(by='concept')
    concept_discrete_df.to_csv(os.path.join(output_dir,
                                            'ddf--concepts--discrete.csv'),
                               index=False)

    # datapackage
    dump_json(os.path.join(output_dir, 'datapackage.json'),
              get_datapackage(output_dir, update=True))


if __name__ == '__main__':
    main()
    print('Done.')
