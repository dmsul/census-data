import os
import zipfile

import pandas as pd
import geopandas as gpd
import simpledbf

from census_data.util import src_path, ftp_connection, get_binary

YEAR = 2012
zipname = f'tl_{YEAR}_us_county.zip'
zippath = src_path('tigerline', 'COUNTY', f'{YEAR}', zipname)


def county_shp() -> pd.DataFrame:
    check_for_files_on_disk()

    df = gpd.read_file(zippath.replace('.zip', '.shp'))

    df = _clean_county_info(df)

    return df


def county_info() -> pd.DataFrame:
    check_for_files_on_disk()

    # NOTE: UTF-8 fails for 2012
    dbf = simpledbf.Dbf5(zippath.replace('.zip', '.dbf'),
                         codec='ISO-8859-1')
    df = dbf.to_dataframe()

    df = _clean_county_info(df)

    return df


def check_for_files_on_disk():
    if not os.path.isfile(zippath):
        try:
            unzip()
        except OSError:
            download()
            unzip()


def _clean_county_info(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns=lambda x: x.lower())
    df = df.drop(['countyfp'], axis=1)
    df = df.rename(columns={'statefp': 'state_fips',
                            'geoid': 'fips',
                            'intptlat': 'y',
                            'intptlon': 'x',
                            'name': 'name_short',
                            'namelsad': 'name'})

    df['state_fips'] = df['state_fips'].astype(int)
    df[['x', 'y']] = df[['x', 'y']].astype(float)

    return df


def download() -> None:
    ftp = ftp_connection(YEAR)
    ftp.cwd('COUNTY')
    fname = ftp.nlst()[0]
    get_binary(fname, src_path('tigerline', 'COUNTY', f'{YEAR}', fname), ftp)


def unzip() -> None:
    root, __ = os.path.split(zippath)
    with open(zippath, 'rb') as f:
        print(f"Unzipping {zipname}...", end='')
        z = zipfile.ZipFile(f)
        z.extractall(path=root)
        print("done.")


if __name__ == "__main__":
    df = county_info()
