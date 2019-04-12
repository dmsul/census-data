import os
import zipfile

import pandas as pd
import geopandas as gpd
import simpledbf

from data_census.util import src_path, ftp_connection, get_binary


def county_shp(year: int) -> pd.DataFrame:
    _check_for_files_on_disk(year)
    zippath = zip_path(year)

    df = gpd.read_file(zippath.replace('.zip', '.shp'))

    df = _clean_county_info(df)

    return df


def county_info(year: int) -> pd.DataFrame:
    _check_for_files_on_disk(year)
    zippath = zip_path(year)

    # NOTE: UTF-8 fails for 2012
    dbf = simpledbf.Dbf5(zippath.replace('.zip', '.dbf'),
                         codec='ISO-8859-1')
    df = dbf.to_dataframe()

    df = _clean_county_info(df)

    return df


# Aux functions
def _check_for_files_on_disk(year: int) -> None:
    zippath = zip_path(year)
    if not os.path.isfile(zippath):
        try:
            _unzip(year)
        except OSError:
            _download(year)
            _unzip(year)


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


def _download(year: int) -> None:
    ftp = ftp_connection(year)
    ftp.cwd('COUNTY')
    fname = ftp.nlst()[0]
    get_binary(fname, src_path('tigerline', 'COUNTY', f'{year}', fname), ftp)
    ftp.close()


def _unzip(year: int) -> None:
    zippath = zip_path(year)
    root, zipname = os.path.split(zippath)
    with open(zippath, 'rb') as f:
        print(f"Unzipping {zipname}...", end='')
        z = zipfile.ZipFile(f)
        z.extractall(path=root)
        print("done.")


def zip_path(year: int) -> str:
    zipname = f'tl_{year}_us_county.zip'
    zippath = src_path('tigerline', 'COUNTY', f'{year}', zipname)
    return zippath


if __name__ == "__main__":
    df = county_info(2012)
