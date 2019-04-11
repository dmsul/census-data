import pandas as pd

from econtools import load_or_build, stata_merge

from census_data.util.env import data_path, src_path
from census_data.clean.demographics.codebooks import codebooks


# Block Group Level Cross Sectional Data
@load_or_build(data_path('bg_demogs_2010.pkl'))
def load_blockgroup() -> pd.DataFrame:
    year = 2010
    bg_nhgis_file = src_path('demographics',
                             'nhgis0014_ds176_20105_2010_blck_grp.csv')
    df = pd.read_csv(bg_nhgis_file, encoding='latin1')

    new_names = codebooks('bg', year)
    df = df[list(new_names.keys())]         # Restrict
    df = df.rename(columns=new_names)       # Rename
    df = df.rename(columns=lambda x: x.lower().replace(' ', '_'))

    id_parts = ['state', 'county', 'tract', 'block_group']
    df['bg'] = (df[id_parts[0]].astype(str).str.zfill(2)
                + df[id_parts[1]].astype(str).str.zfill(3)
                + df[id_parts[2]].astype(str).str.zfill(6)
                + df[id_parts[3]].astype(str))
    df = df.drop(id_parts, axis=1)
    df = df.set_index('bg')

    df['year'] = year

    return df


@load_or_build(data_path('bg_demogs_{year}.p'))
def bg_demogs(year: int) -> pd.DataFrame:

    assert year in (2000, 2005)
    if year == 2000:
        rawdata = _combine_two_2000_files()
    else:
        nhgis_path = src_path('demographics',
                              'nhgis0017_ds195_20095_2009_blck_grp.csv')
        rawdata = pd.read_csv(nhgis_path)

    # Rename useful variables
    new_names = codebooks('bg', year)
    rawdata.rename(columns=new_names, inplace=True)

    # Restrict columns
    rawdata.rename(columns=lambda x: x.lower(), inplace=True)
    if year == 2000:
        rawdata.rename(columns={'blck_grpa': 'blkgrpa'}, inplace=True)
    bg = ['statea', 'countya', 'tracta', 'blkgrpa']
    data = rawdata[bg + list(new_names.values())].copy()

    _fix_errata(data, year)

    # Make BG ID
    data['bg'] = (data['statea'].astype(str).str.zfill(2)
                  + data['countya'].astype(str).str.zfill(3)
                  + data['tracta'].astype(str).str.zfill(6)
                  + data['blkgrpa'].astype(str))
    data.drop(bg, axis=1, inplace=True)

    # Set year
    data['year'] = year

    data = data.set_index('bg')

    return data

def _combine_two_2000_files() -> pd.DataFrame:
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

def _fix_errata(df: pd.DataFrame, year: int) -> None:
    """ Should modify `df` in place. """
    if year == 2005:
        df['hunit'] = df['households'] + df['hunit_vacant']


if __name__ == '__main__':
    df = bg_demogs(2000, _rebuild=True)
    df2 = bg_demogs(2005, _rebuild=True)
    # df = load_blockgroup(_load=False)
