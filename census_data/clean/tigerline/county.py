import os
import zipfile
import simpledbf

import pandas as pd

from census_data.util.env import src_path
from census_data.util.ftp import ftp_connection, get_binary

YEAR = 2012
filename = f'tl_{YEAR}_us_county.zip'
filepath = src_path('tigerline', 'COUNTY', f'{YEAR}', filename)


def county_info() -> pd.DataFrame:
    dbf = simpledbf.Dbf5(
        'd:/data/census/src/tigerline/COUNTY/2012/tl_2012_us_county.dbf',
        codec='ISO-8859-1')
    df = dbf.to_dataframe()
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

    root, __ = os.path.split(filepath)
    with open(filepath, 'rb') as f:
        print(f"Unzipping {filename}...", end='')
        z = zipfile.ZipFile(f)
        z.extractall(path=root)
        print("done.")


if __name__ == "__main__":
    df = county_info()
