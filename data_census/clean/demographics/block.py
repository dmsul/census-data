import pandas as pd
import numpy as np
from econtools import load_or_build

from data_census.util.env import src_path, data_path
from data_census.clean.demographics.codebooks import codebooks


@load_or_build(data_path('blocks_pop_{year}.pkl'))
def blocks_population(year: int):
    if year == 2010:
        df = block_demogs()
        df.index = block_id(df)
        df.index.name = 'block_id'
        df = df['population']
        df.name = 'pop'
    elif year == 2000:
        df = block_pop_2000()

    return df


@load_or_build(data_path('block_demogs_{year}.pkl'))
def block_demogs(year: int):
    assert year == 2010

    csv_path = src_path('census', 'cross_sectional_data',
                        'nhgis_2010_block', 'nhgis0013_ds172_2010_block.csv')
    new_names = codebooks('block', year)
    columns = list(new_names.keys())
    df = pd.DataFrame()
    chunk_size = 10 ** 6
    for chunk_df in pd.read_csv(csv_path, chunksize=chunk_size):
        print('next chunk')
        chunk_df = chunk_df[columns]
        chunk_df = chunk_df.rename(columns=new_names)
        chunk_df = _age_bins(chunk_df)
        chunk_df = _cast_dtype(chunk_df)
        df = df.append(chunk_df)
        del chunk_df

    # completing race variables
    df['race_black'] = df['race_black_no_hisp'] + df['race_black_hisp']
    df['race_native'] = df['race_native_no_hisp'] + df['race_native_hisp']
    df['race_asian'] = df['race_asian_no_hisp'] + df['race_asian_hisp']
    df['race_island'] = df['race_island_no_hisp'] + df['race_island_hisp']
    df['race_other'] = df['race_other_no_hisp'] + df['race_other_hisp']
    df['race_two_or_more'] = (
        df['race_two_or_more_no_hisp'] + df['race_two_or_more_hisp']
    )

    df = df.drop(['race_black_no_hisp', 'race_native_no_hisp',
                  'race_asian_no_hisp', 'race_island_no_hisp',
                  'race_other_no_hisp', 'race_two_or_more_no_hisp',
                  'race_black_hisp', 'race_native_hisp', 'race_asian_hisp',
                  'race_island_hisp', 'race_other_hisp',
                  'race_two_or_more_hisp'], axis=1)

    return df

def _age_bins(df):
    """ Consolodate age bins to save memory """
    df['age_9'] = (
        df[[f'age_{g}_{a}'
            for g in ('male', 'female')
            for a in (4, 9)]].sum(axis=1))
    df['age_19'] = (
        df[[f'age_{g}_{a}'
            for g in ('male', 'female')
            for a in (14, 17, 19)]].sum(axis=1))
    df['age_29'] = (
        df[[f'age_{g}_{a}'
            for g in ('male', 'female')
            for a in (20, 21, 24, 29)]].sum(axis=1))
    df['age_39'] = (
        df[[f'age_{g}_{a}'
            for g in ('male', 'female')
            for a in (34, 39)]].sum(axis=1))
    df['age_49'] = (
        df[[f'age_{g}_{a}'
            for g in ('male', 'female')
            for a in (44, 49)]].sum(axis=1))
    df['age_59'] = (
        df[[f'age_{g}_{a}'
            for g in ('male', 'female')
            for a in (54, 59)]].sum(axis=1))
    df['age_69'] = (
        df[[f'age_{g}_{a}'
            for g in ('male', 'female')
            for a in (61, 64, 66, 69)]].sum(axis=1))
    df['age_79'] = (
        df[[f'age_{g}_{a}'
            for g in ('male', 'female')
            for a in (74, 79)]].sum(axis=1))
    df['age_80_over'] = (
        df[[f'age_{g}_{a}'
            for g in ('male', 'female')
            for a in (84, 99)]].sum(axis=1))

    df = df.drop([f'age_{g}_{a}'
                  for g in ('male', 'female')
                  for a in (4, 9, 14, 17, 19, 20, 21, 24, 29, 34, 39, 44, 49,
                            54, 61, 64, 66, 69, 74, 79, 84, 99)
                  ], axis=1)

    return df

def _cast_dtype(df):
    cast_to_32 = ('tract',)
    cast_to_8 = ('state', 'block_group')
    cast_to_16 = tuple(
        [x for x in df.columns if x not in cast_to_32 + cast_to_8]
    )

    cast_to = (
        (cast_to_32, np.uint32),
        (cast_to_16, np.uint16),
        (cast_to_8, np.uint8),
    )
    for cast_list, dtype_target in cast_to:
        for col in cast_list:
            df[col] = df[col].astype(dtype_target)

    return df


def block_pop_2000():
    """ Year 2000 pop, block-level """

    nhgis_path = src_path('demographics', 'nhgis0009_ds147_2000_block.csv')

    usecols = ['STATEA', 'COUNTYA', 'TRACTA', 'BLOCKA', 'FXS001']
    df = pd.read_csv(nhgis_path, usecols=usecols, header=0)

    df = df.rename(columns={'FXS001': 'pop2000'})

    df = df.rename(columns=lambda x: x.lower())

    df['fips'] = (df['statea'].astype(str).str.zfill(2)
                  + df['countya'].astype(str).str.zfill(3))
    df['block_id'] = (df['fips']
                      + df['tracta'].astype(str).str.zfill(6)
                      + df['blocka'].astype(str).str.zfill(4))

    df = df.drop(['statea', 'countya', 'tracta', 'blocka', 'fips'])

    df = df.set_index('block_id')

    return df


def block_id(df, state='state', county='county', tract='tract', block='block'):
    b_id = (
        df[state].astype(str).str.zfill(2) +
        df[county].astype(str).str.zfill(3) +
        df[tract].astype(str).str.zfill(6) +
        df[block].astype(str).str.zfill(4)
    )
    return b_id


if __name__ == "__main__":
    pass
