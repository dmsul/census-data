import urllib.request
import time
import os
import zipfile

import geopandas as gpd
from econtools import state_fips_list

from data_census.util.env import src_path


def bg_shape_df(state_fips) -> gpd.DataFrame:
    shp_path = _bg_shape_path(state_fips)
    if not os.path.isfile(shp_path):
        _unzip_bg_shapefile(state_fips)
    df = gpd.read_file(shp_path)
    return df


def _unzip_bg_shapefile(state_fips: str) -> None:
    print(f"Unzipping state {state_fips} block group shapefiles...", end='')
    zip_path = _bg_zip_path(state_fips)
    print(zip_path)
    zip_ref = zipfile.ZipFile(zip_path)
    zip_ref.extractall(os.path.split(_bg_shape_path(state_fips))[0])
    zip_ref.close()
    print("Done.")

def _bg_shape_path(state_fips: str) -> str:
    folder, filename = os.path.split(_bg_zip_path(state_fips))
    shp_path = os.path.join(folder, '..', filename.replace('.zip', '.shp'))
    return shp_path

def _bg_zip_path(state_fips: str) -> str:
    zip_path = src_path('census', 'shapefile_blockgroup', 'zipped',
                        f'gz_2010_{state_fips}_150_00_500k.zip')
    return zip_path


def _download_bg_shapefile(state_fips: str, resolution: str) -> None:
    target_folder = src_path('cartographic_shapes')
    bg_url = (
        'https://www2.census.gov/geo/' +
        f'tiger/GENZ2010/gz_2010_{state_fips}_150_00_{resolution}.zip'
    )
    zip_file_name = os.path.split(bg_url)[1]
    zip_file_path = os.path.join(target_folder, zip_file_name)
    if os.path.isfile(zip_file_path):
        print(f'{zip_file_name} already on disk')
        return
    else:
        urllib.request.urlretrieve(bg_url, zip_file_path)


if __name__ == '__main__':
    resolution = '500k'
    import sys
    if len(sys.argv) == 2:
        state_fips = sys.argv[1]
        _download_bg_shapefile(state_fips, resolution)
    else:
        state_codes = [str(x).zfill(2) for x in state_fips_list]
        for i in state_codes:
            _download_bg_shapefile(i, resolution)
            time.sleep(3)
