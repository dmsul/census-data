import os
import zipfile
import simpledbf

import numpy as np
import pandas as pd
import geopandas as gpd
from econtools import load_or_build, state_fips_list, confirmer

from data_census.util import src_path, data_path
from data_census.clean.tigerline.block_download import (
    state_fileroot, download_state_shp)


# Block functions
@load_or_build(data_path('block_shape_info_{year}.pkl'))
def block_shape_info(year: int) -> pd.DataFrame:
    dfs = [block_shape_info_state(str(state_fips).zfill(2), year)
           for state_fips in state_fips_list]
    df = pd.concat(dfs)
    return df


def block_shape_info_state(state_fips: str,
                           year: int,
                           vintage: int=None,
                           ) -> pd.DataFrame:
    """
    Note: Last vintage for Census 2000 data is 2010.
    """
    set_vintage = _parse_vintage(year, vintage)
    df = _load_state_dbf(year, set_vintage, state_fips)
    rename = _rename(year, set_vintage)
    df = df.rename(columns=rename)
    df = df[list(rename.values())].copy().set_index('block_id')

    df = _recast_dtypes(df)
    return df

def _load_state_dbf(year: int, vintage: int, state_fips: str) -> pd.DataFrame:
    dbf_path = (_blocks_shape_path(year, vintage, state_fips)
                .replace('.shp', '.dbf'))
    if not os.path.isfile(dbf_path):
        _unzip_block_dbf(year, vintage, state_fips)
    df = simpledbf.Dbf5(dbf_path).to_dataframe()
    return df

def _unzip_block_dbf(year: int, vintage: int, state_fips: str) -> None:
    print(f"Unzipping {state_fips} DBF only...", end='')
    zip_path = _blocks_zip_path(year, vintage, state_fips)
    zip_obj = zipfile.ZipFile(zip_path)
    target_path = os.path.split(zip_path)[0]
    for info in zip_obj.infolist():
        if info.filename.endswith('.dbf'):
            print("Found {info.filename}")
            zip_obj.extract(info, path=target_path)
    zip_obj.close()
    print("Done.")


def block_shape_state(state_fips: str,
                      year: int,
                      vintage: int=None
                      ) -> gpd.GeoDataFrame:
    set_vintage = _parse_vintage(year, vintage)
    shp_path = _blocks_shape_path(year, set_vintage, state_fips)
    if not os.path.isfile(shp_path):
        _unzip_block_shp(year, set_vintage, state_fips)

    df = gpd.read_file(shp_path)
    df = df.rename(columns=_rename(year, set_vintage))
    df = _recast_dtypes(df)

    return df


def _parse_vintage(year: int, vintage: int=None) -> int:
    if vintage is None:
        vintage = 2010 if year == 2000 else 2010
    elif year == 2000 and vintage != 2010:
        raise ValueError

    return vintage


def _rename(year: int, vintage: int) -> dict:
    year_suffix = str(year)[-2:]
    if year == 2010:
        block_id_name = f'GEOID{year_suffix}'
    elif year == 2000:
        block_id_name = f'BLKIDFP{year_suffix}'

    rename = {
        block_id_name: 'block_id',
        f'INTPTLAT{year_suffix}': 'y',
        f'INTPTLON{year_suffix}': 'x',
        f'ALAND{year_suffix}': 'area',
    }
    return rename


def _recast_dtypes(df):
    # Leading + and - make these strings; fix that.
    df[['x', 'y']] = df[['x', 'y']].astype(np.float32)
    df['area'] = df['area'].astype(np.int32)    # compress

    return df


def _unzip_block_shp(year: int, vintage: int, state_fips: str) -> None:
    # needs to unzip all files in folder to load .shp later
    zip_path = _blocks_zip_path(year, vintage, state_fips)
    try:
        zip_ref = zipfile.ZipFile(zip_path)
    except FileNotFoundError as e:
        ans = confirmer(f"File {zip_path} not found. Download?")
        if ans:
            download_state_shp(year, vintage, state_fips)
            zip_ref = zipfile.ZipFile(zip_path)
        else:
            raise e
    zips_folder = os.path.split(zip_path)[0]
    print(f"Unzipping state {state_fips} to {zips_folder}", end='')
    zip_ref.extractall(zips_folder)
    zip_ref.close()
    print(" Done.")


def _blocks_shape_path(year: int, vintage: int, state_fips: str) -> str:
    filename = state_fileroot(year, vintage, state_fips) + '.shp'
    shp_path = src_path('tigerline', 'BLOCK', str(year), filename)
    return shp_path


def _blocks_zip_path(year: int, vintage: int, state_fips: str) -> str:
    filename = state_fileroot(year, vintage, state_fips) + '.zip'
    zip_path = src_path('tigerline', 'BLOCK', str(year), filename)
    return zip_path
