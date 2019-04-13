import os

import geopandas as gpd

from data_census.util import src_path, file_download, unzip_file

# TODO: This is close to generalized for everything beyond states 

RESOLUTIONS = ('500k', '5m', '20m')     # High-res to low-res
VALID_YEARS = (1990, 2000, 2010, 2013, 2014, 2015, 2016)


def state_shape_df(year: int, resolution: str) -> gpd.GeoDataFrame:
    shp_path = carto_shapes_src_path(
        file_root(year, resolution, 'state', 'us'))
    _download_if_needed(shp_path)
    df = gpd.read_file(shp_path)
    return df

def _download_if_needed(shp_path: str) -> None:
    # Download and unzip, if necessary
    if not os.path.isfile(shp_path):
        zip_path = shp_path.replace('.shp', '.zip')
        if not os.path.isfile(zip_path):
            _download_zipfile(zip_path, year)
        unzip_file(zip_path)

def _download_zipfile(zip_path: str, year: int) -> None:
    __, zipname = os.path.split(zip_path)
    full_url = cartographic_url_year(year) + zipname
    file_download(full_url, zip_path)


def file_root(year: int, resolution: str, unit: str, span: str) -> str:
    """ `span` is 'us' or a state fips code """
    if year == 2010:
        prefix = 'gz'
        infix = _unit_infix_2010
    else:
        prefix = 'cb'
        infix = _unit_infix_post2010

    fileroot = f'{prefix}_{year}_{span}_{infix(unit)}_{resolution}.shp'

    return fileroot

def _unit_infix_2010(unit: str) -> str:
    xwalk = {
        'nation': 'outline',
        'state': '040_00',
        'county': '050_00',
        'subcounty': '060_00',
        'zcta': '860_00',
        'tract': '140_00',
        'bg': '150_00',
    }

    return xwalk[unit]

def _unit_infix_post2010(unit: str) -> str:
    if unit == 'zcta': return 'zcta510'
    elif unit == 'subcounty': return 'cousub'
    else: return unit


def cartographic_url_year(year: int) -> str:
    return f'http://www2.census.gov/geo/tiger/GENZ{year}/'


def carto_shapes_src_path(filename: str) -> str:
    return src_path('cartographic_shapes', filename)


if __name__ == "__main__":
    year = 2010
    resolution = '5m'

    df = state_shape_df(year, resolution)
