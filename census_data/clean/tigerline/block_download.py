import os
import time
import ftplib

from census_data.util import src_path, ftp_connection


# This is all just downloading the zip files
SLEEP = 1

def download_all_state_files() -> list:
    year = 2010
    ftp = wrap_ftp_connection(year)
    state_files = _get_list_of_state_files(ftp)

    for fname in state_files:
        target_path = files_target_path(year, fname)
        # If target file already downloaded, skip
        if os.path.exists(target_path):
            continue
        else:
            print(f"Downloading {fname}...", end='', flush=True)
            ftp = wrap_download(ftp, fname, target_path)

    return state_files

def _get_list_of_state_files(ftp) -> list:
    all_files = ftp.nlst()
    # Grab all the shorter (state- not county-level) filenames
    state_files = [x for x in all_files if len(x) < 27]
    return state_files


def download_state_shp(year: int, state_fips: int, ftp=None) -> None:
    pass


def wrap_download(ftp, filename, target_path):
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
        wrap_download(ftp, filename, target_path)
    except (ftplib.error_reply, ftplib.error_temp, ConnectionResetError):
        print("Connection error! ", end='')
        ftp = _reset_ftp(ftp)
        wrap_download(ftp, filename, target_path)
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
    pass
