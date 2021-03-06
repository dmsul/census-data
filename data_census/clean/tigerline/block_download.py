import os
import time
import ftplib

from econtools import state_fips_list

from data_census.util import src_path, ftp_connection

SLEEP = 1


def download_all_state_files() -> None:
    year = 2010
    ftp = wrap_ftp_connection(year)     # Use one ftp connection
    str_fips = [str(fips).zfill(2) for fips in state_fips_list]

    for state_fips in str_fips:
        ftp = download_state_shp(year, state_fips, ftp=ftp)


def download_state_shp(year: int, state_fips: str, ftp=None) -> ftplib.FTP:
    """
    Download the state's shape file. Can be used independently or in loop.
    """
    fname = _state_zip_filename(year, state_fips)
    target_path = files_target_path(year, fname)
    # If target file already downloaded, skip
    if ftp is None:
        ftp = wrap_ftp_connection(year)

    if os.path.exists(target_path):
        pass
    else:
        print(f"Downloading {fname}...", end='', flush=True)
        ftp = _wrap_download(ftp, fname, target_path)

    # return `ftp` for next loop iteration b/c `wrap_download` might reset it
    return ftp

def _state_zip_filename(year: int, fips: str) -> str:
    """
    This is not exactly right. First `year` is data vintage. Second `year` is
    which census the data corresponds to.
    """
    return f'tl_{year}_{fips}_tabblock{str(year)[-2:]}.zip'

def _wrap_download(ftp, filename, target_path):
    try:
        _get_binary(ftp, filename, target_path)
    except KeyboardInterrupt:
        print("Interrupt!")
        ftp.quit()
        os.remove(target_path)      # XXX doesn't work, file locked
        raise
    except StopIteration:
        print("Timeout! ", end='')
        ftp = _reset_ftp(ftp)
        _wrap_download(ftp, filename, target_path)
    except (ftplib.error_reply, ftplib.error_temp, ConnectionResetError):
        print("Connection error! ", end='')
        ftp = _reset_ftp(ftp)
        _wrap_download(ftp, filename, target_path)
    finally:
        time.sleep(SLEEP)

    return ftp

def _reset_ftp(year, day, ftp):
    print('Reseting FTP connection...', end='')
    ftp.close()
    ftp = wrap_ftp_connection()
    return ftp

def _get_binary(ftp, filename, target_path):
    with open(target_path, 'wb') as open_f:
        ftp.retrbinary(f"RETR {filename}", open_f.write)
    print('done!')


def wrap_ftp_connection(year: int) -> ftplib.FTP:
    """ Establish FTP connection, navigate to MODIS 3K folder """
    ftp = ftp_connection(year)
    ftp.cwd(f'TABBLOCK/{year}')

    return ftp


def files_target_path(year: int, f_name: str) -> str:
    return src_path('tigerline', 'BLOCK', f'{year}', f_name)


if __name__ == '__main__':
    download_all_state_files()
