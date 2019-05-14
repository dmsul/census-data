import os
import urllib.request as request
import ftplib
import zipfile


def file_download(source_url: str, target_path: str) -> None:
    print(f"Downloading {source_url}...", end='')
    request.urlretrieve(source_url, target_path)
    print('done!')


def unzip_file(zippath: str, target_dir=None) -> None:
    root, zipname = os.path.split(zippath)
    with open(zippath, 'rb') as f:
        print(f"Unzipping {zipname}...", end='')
        z = zipfile.ZipFile(f)
        if not target_dir:
            target_dir = root
        z.extractall(path=target_dir)
        print("done.")


def ftp_connection(vintage: int) -> ftplib.FTP:
    """ Establish FTP connection, navigate to `year` folder """
    FTP_DOMAIN = r'ftp2.census.gov'
    ftp = ftplib.FTP(FTP_DOMAIN)
    ftp.login()
    ftp.cwd(f'geo/tiger/TIGER{vintage}')

    return ftp


def get_binary(filename: str, target_path: str, ftp: ftplib.FTP) -> None:
    print(f"Downloading {filename}...", end='')
    with open(target_path, 'wb') as open_f:
        ftp.retrbinary(f"RETR {filename}", open_f.write)
    print('done!')
