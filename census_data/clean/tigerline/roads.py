import os
import ftplib
import glob
import zipfile

import pandas as pd
import geopandas as gpd
from econtools import state_abbr_to_name, state_name_to_fips

from census_data.util import src_path, ftp_connection, get_binary
from census_data.clean.tigerline.county import county_info


def read_year_state_roads(state_abbr: str, year: int) -> gpd.GeoDataFrame:
    fips_list = _get_states_fips_list(state_abbr)
    dfs = [read_year_county_roads(fips, year) for fips in fips_list]
    return pd.concat(dfs, axis=0)

def _get_states_fips_list(state_abbr: str) -> list:
    state_fips = state_abbr_to_fips(state_abbr)
    df = county_info()
    return df.loc[df['state_fips'] == state_fips, 'fips'].unique().tolist()


def read_year_county_roads(fips: str, year: int) -> gpd.GeoDataFrame:
    return gpd.read_file(shp_path(year, fips))


# Aux functions
def batch_unzip_year_state(state_abbr: str, year: int) -> None:
    file_list = glob.glob(os.path.join(zip_path_root(year), '*'))
    state_list = restrict_filelist_to_state(file_list, state_abbr)
    for filepath in state_list:
        with open(filepath, 'rb') as f:
            print(f"Unzipping {os.path.split(filepath)[1]}...", end='')
            z = zipfile.ZipFile(f)
            z.extractall(path=year_path_root(year))
            print("done.")


def batch_download_year_state(state_abbr: str, year: int) -> None:
    ftp = _ftp_connection(year)

    # File format is 'tl_{year}_{fips}_roads.zip'
    file_list = ftp.nlst()
    state_list = restrict_filelist_to_state(file_list, state_abbr)

    target = os.path.join(zip_path_root(year), '{filename}')
    for f in state_list:
        get_binary(f, target.format(filename=f), ftp)

    ftp.close()

def _ftp_connection(year: int) -> ftplib.FTP:
    """ Establish FTP connection, navigate to MODIS 3K folder """
    ftp = ftp_connection(year)
    ftp.cwd('ROADS')

    return ftp


def restrict_filelist_to_state(file_list: list, state_abbr: str) -> list:
    state_fips = str(state_abbr_to_fips(state_abbr)).zfill(2)
    state_list = [x for x in file_list if x[-15:-13] == state_fips]
    return state_list


def state_abbr_to_fips(state_abbr: str) -> int:
    return state_name_to_fips(state_abbr_to_name(state_abbr))


def shp_path(year: int, fips: str) -> str:
    return os.path.join(
        year_path_root(year),
        f'tl_{year}_{fips}_roads.shp')


def zip_path_root(year: int) -> str:
    return os.path.join(year_path_root(year), 'zips')


def year_path_root(year: int) -> str:
    return src_path('tigerline', 'ROADS', f'{year}')


if __name__ == "__main__":
    year = 2012
    state_abbr = 'PA'
    # batch_download_year_state(state_abbr, year)
    df = read_year_state_roads('PA', year)
