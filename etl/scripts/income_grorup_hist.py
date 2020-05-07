"""income group history datapoints"""

from os.path import join
import pandas as pd

source_dir = '../source'
output_dir = '../../'
oghist_file = join(source_dir, 'OGHIST.xls')


def flatten_table(table: pd.DataFrame):
    return table.stack().dropna().reset_index()


def load_and_pre_process(source_file):
    table = pd.read_excel(oghist_file,
                          sheet_name='Country Analytical History',
                          encoding='latin',
                          keep_default_na=False,
                          na_values=['', '..'],
                          skiprows=5)
    table = table.drop(table.columns[1], axis=1)
    table = table.rename(columns={'Unnamed: 0': 'economy'})
    table['economy'] = table['economy'].str.lower()
    table = table.set_index('economy').iloc[4:]
    return table


def remap_df(df, column, column_new_name, mapping):
    df = df.copy()
    df[column] = df[column].map(lambda v: mapping[v.strip()])
    df = df.rename(columns={column: column_new_name})
    return df


def create_hist_income_grorup_datapoints(table: pd.DataFrame):
    df = flatten_table(table)
    df.columns = ['economy', 'year', 'income_group']
    # NOTE from source file:
    # * At this time, there were Yemen, PDR (L) and Yemen, Arab Rep. (LM);
    # combined they would have been LM.
    level_map_4 = {
        'L': 'lic',
        'LM': 'lmc',
        'LM*': 'lmc',
        'UM': 'umc',
        'H': 'hic'
    }
    level_map_3 = {
        'L': 'lic',
        'LM': 'mic',
        'LM*': 'mic',
        'UM': 'mic',
        'H': 'hic'
    }
    level_map_2 = {
        'L': 'lmy',
        'LM': 'lmy',
        'LM*': 'lmy',
        'UM': 'lmy',
        'H': 'hic'
    }

    df4lvl = remap_df(df, 'income_group', 'income_4level', level_map_4)
    df3lvl = remap_df(df, 'income_group', 'income_3level', level_map_3)
    df2lvl = remap_df(df, 'income_group', 'income_2level', level_map_2)

    return dict(income_4level=df4lvl,
                income_3level=df3lvl,
                income_2level=df2lvl)


if __name__ == '__main__':
    table = load_and_pre_process(oghist_file)
    result = create_hist_income_grorup_datapoints(table)
    for lvl, df in result.items():
        df.to_csv(join(output_dir,
                       f'ddf--datapoints--{lvl}--by--economy--year.csv'),
                  index=False)
