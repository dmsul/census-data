import urllib.request
import time
import os
import zipfile

import geopandas as gpd
from econtools import state_fips_list

from census_data.util import src_path


def tract_shape_df(state_fips: str) -> gpd.DataFrame:
    shp_path = _tracts_shape_path(state_fips)
    if not os.path.isfile(shp_path):
        _unzip_tract_shapefile(state_fips)
    df = gpd.read_file(shp_path)
    return df


def _unzip_tract_shapefile(state_fips: str) -> None:
    print(f"Unzipping state {state_fips} tract shapefiles...", end='')
    zip_path = _tracts_zip_path(state_fips)
    print(zip_path)
    zip_ref = zipfile.ZipFile(zip_path)
    zip_ref.extractall(os.path.split(_tracts_shape_path(state_fips))[0])
    zip_ref.close()
    print("Done.")

def _tracts_shape_path(state_fips: str) -> str:
    folder, filename = os.path.split(_tracts_zip_path(state_fips))
    shp_path = os.path.join(folder, '..', filename.replace('.zip', '.shp'))
    return shp_path

def _tracts_zip_path(state_fips: str) -> str:
    zip_path = src_path('census', 'census_tract_shapefiles_2010', 'zipped',
                        f'gz_2010_{state_fips}_140_00_500k.zip')
    return zip_path


def download_tract_shapefile(state_fips: str, resolution: str) -> None:
    target_folder = src_path('cartographic_shapes')
    ct_url = (
        'http://www2.census.gov/geo/' +
        f'tiger/GENZ2010/gz_2010_{state_fips}_140_00_{resolution}.zip'
    )
    zip_file_name = os.path.split(ct_url)[1]
    zip_file_path = os.path.join(target_folder, zip_file_name)
    if os.path.isfile(zip_file_path):
        print(f'{zip_file_name} already on disk')
        return
    else:
        urllib.request.urlretrieve(ct_url, zip_file_path)


if __name__ == '__main__':
    resolution = '500k'
    import sys
    if len(sys.argv) == 2:
        state_fips = sys.argv[1]
        download_tract_shapefile(state_fips, resolution)
    else:
        state_codes = [str(x).zfill(2) for x in state_fips_list]
        for i in state_codes:
            download_tract_shapefile(i, resolution)
            time.sleep(3)
