import pandas as pd

from econtools import load_or_build, stata_merge

from data_census.util.env import data_path, src_path
from data_census.clean.demographics.codebooks import codebooks


# Block Group Level Cross Sectional Data
@load_or_build(data_path('bg_demogs_{year}.pkl'))
def blockgroup_demogs(year: int) -> pd.DataFrame:
    df = _load_raw_data(year)

    new_names = codebooks('bg', year)
    df = df[list(new_names.keys())]         # Restrict
    df = df.rename(columns=new_names)       # Rename

    id_parts = ['state', 'county', 'tract', 'block_group']
    df['bg'] = (df[id_parts[0]].astype(str).str.zfill(2)
                + df[id_parts[1]].astype(str).str.zfill(3)
                + df[id_parts[2]].astype(str).str.zfill(6)
                + df[id_parts[3]].astype(str))
    df = df.drop(id_parts, axis=1)
    df = df.set_index('bg')

    df['year'] = year

    df = _fix_errata(df, year)

    return df

def _load_raw_data(year: int) -> pd.DataFrame:
    if year == 2000:
        df = __combine_two_2000_files()
    elif year == 2005:
        nhgis_path = src_path('demographics',
                              'nhgis0017_ds195_20095_2009_blck_grp.csv')
        df = pd.read_csv(nhgis_path)
    elif year == 2010:
        nhgis_file = src_path('demographics',
                              'nhgis0014_ds176_20105_2010_blck_grp.csv')
        df = pd.read_csv(nhgis_file, encoding='latin1')

    return df

def __combine_two_2000_files() -> pd.DataFrame:
    """
    NHGIS outputs the 2000 Census in two files for some reason. Combine
    them here.
    """
    file1 = src_path('demographics', 'nhgis0020_ds152_2000_blck_grp.csv')
    file2 = src_path('demographics', 'nhgis0020_ds147_2000_blck_grp.csv')
    tmp1 = pd.read_csv(file1)
    tmp2 = pd.read_csv(file2)
    # Drop all duplicate columns except gisjoin
    cols1 = set(tmp1.columns)
    cols2 = set(tmp2.columns)
    keep_set = cols1.symmetric_difference(cols2)
    bg = ['STATEA', 'COUNTYA', 'TRACTA', 'BLCK_GRPA']
    keep_cols1 = [x for x in tmp1.columns if x in keep_set]
    keep_cols2 = [x for x in tmp2.columns if x in keep_set]
    tmp1 = tmp1[['GISJOIN'] + bg + keep_cols1]
    tmp2 = tmp2[['GISJOIN'] + keep_cols2]
    rawdata = stata_merge(tmp1, tmp2, on='GISJOIN', assertval=3)
    return rawdata

def _fix_errata(df: pd.DataFrame, year: int) -> pd.DataFrame:
    if year == 2005:
        df['hunit'] = df['households'] + df['hunit_vacant']

    return df


if __name__ == '__main__':
    df = blockgroup_demogs(2000, _rebuild=True)
    df2 = blockgroup_demogs(2005, _rebuild=True)
    df3 = blockgroup_demogs(2010, _rebuild=True)
