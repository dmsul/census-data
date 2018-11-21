import ftplib


def ftp_connection(year: int) -> ftplib.FTP:
    """ Establish FTP connection, navigate to MODIS 3K folder """
    FTP_DOMAIN = r'ftp2.census.gov'
    ftp = ftplib.FTP(FTP_DOMAIN)
    ftp.login()
    ftp.cwd(f'geo/tiger/TIGER{year}')

    return ftp


def get_binary(filename: str, target_path: str, ftp: ftplib.FTP) -> None:
    print(f"Downloading {filename}...", end='')
    with open(target_path, 'wb') as open_f:
        ftp.retrbinary(f"RETR {filename}", open_f.write)
    print('done!')
