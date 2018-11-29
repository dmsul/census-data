import os
import geopandas as gpd

from census_data.util.env import src_path
from census_data.util.ftp import file_download, unzip_file


def state_shape_df(year: int, resolution: str) -> gpd.GeoDataFrame:
    shp_path = local_src_path(file_root(year, resolution) + '.shp')

    # Download and unzip, if necessary
    if not os.path.isfile(shp_path):
        zip_path = shp_path.replace('.shp', '.zip')
        if not os.path.isfile(zip_path):
            download_zipfile(zip_path, year)
        unzip_file(zip_path)

    df = gpd.read_file(shp_path)
    return df


def download_zipfile(zip_path: str, year: int) -> None:
    __, zipname = os.path.split(zip_path)
    full_url = url_year(year) + zipname
    file_download(full_url, zip_path)


def local_src_path(filename: str) -> str:
    return src_path('cartographic_shapes', filename)


def file_root(year: int, resolution: str) -> str:
    if year == 2010:
        fileroot = f'gz_{year}_us_040_00_{resolution}'
    else:
        fileroot = f'cb_{year}_us_state_{resolution}'

    return fileroot


def url_year(year: int) -> str:
    return f'http://www2.census.gov/geo/tiger/GENZ{year}/'


if __name__ == "__main__":
    year = 2010
    resolution = '5m'

    df = state_shape_df(year, resolution)
