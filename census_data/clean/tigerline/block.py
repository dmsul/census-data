import os
import zipfile
import simpledbf

import numpy as np
import pandas as pd
import geopandas as gpd
from econtools import load_or_build

from census_data.util import fips_to_name_xwalk
from census_data.util import src_path, data_path


# Block functions
@load_or_build(data_path('blocks_shape_info.pkl'))
def load_blocks_shape_info() -> pd.DataFrame:
    dfs = [load_blocks_shape_info_state(state)
           for state in list(fips_to_name_xwalk.keys())]
    df = pd.concat(dfs)
    return df


@load_or_build(data_path('blocks_shape_info_{}.pkl'), path_args=[0])
def load_blocks_shape_info_state(state_fips: str) -> pd.DataFrame:
    df = _load_block_dbf(state_fips)
    rename = {
        'GEOID10': 'block_id',
        'INTPTLAT10': 'y',
        'INTPTLON10': 'x',
        'ALAND10': 'area',
    }
    df = df.rename(columns=rename)
    df = df[list(rename.values())].copy().set_index('block_id')

    # Leading + and - make these strings; fix that.
    df[['x', 'y']] = df[['x', 'y']].astype(np.float32)
    df['area'] = df['area'].astype(np.int32)    # compress

    return df


def _load_block_dbf(state_fips: str) -> pd.DataFrame:
    dbf_path = _blocks_shape_path(state_fips).replace('.shp', '.dbf')
    if not os.path.isfile(dbf_path):
        unzip_block_dbf(state_fips)
    df = simpledbf.Dbf5(dbf_path).to_dataframe()
    return df


def load_block_shape(state_fips: str) -> pd.DataFrame:
    shp_path = _blocks_shape_path(state_fips)
    if not os.path.isfile(shp_path):
        unzip_block_shp(state_fips)
    df = gpd.read_file(shp_path)
    return df


def unzip_block_dbf(state_fips):
    print(f"Unzipping {state_fips} DBF only...", end='')
    zip_path = _blocks_zip_path(state_fips)
    zip_obj = zipfile.ZipFile(zip_path)
    target_path = os.path.split(_blocks_zip_path(state_fips))[0]
    for info in zip_obj.infolist():
        if info.filename.endswith('.dbf'):
            print("Found")
            print(info.filename)
            zip_obj.extract(info, path=target_path)
    zip_obj.close()
    print("Done.")


def unzip_block_shp(state_fips):
    # needs to unzip all files in folder to load .shp later
    zip_path = _blocks_zip_path(state_fips)
    zip_ref = zipfile.ZipFile(zip_path)
    zips_folder = os.path.split(_blocks_zip_path(state_fips))[0]
    print(f"Unzipping state {state_fips} to\n{zips_folder}")
    zip_ref.extractall(zips_folder)
    zip_ref.close()
    print("Done.")


def _blocks_shape_path(state_fips: str) -> str:
    shp_path = src_path('census', 'shapefile_block',
                        f'tl_2010_{state_fips}_tabblock10.shp')
    return shp_path

def _blocks_zip_path(state_fips):
    zip_path = src_path('census', 'shapefile_block',
                        f'tl_2010_{state_fips}_tabblock10.zip')
    return zip_path
