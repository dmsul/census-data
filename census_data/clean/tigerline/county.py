from census_data.util.env import src_path
from census_data.util.ftp import ftp_connection, get_binary


if __name__ == "__main__":
    ftp = ftp_connection(2012)
    ftp.cwd('COUNTY')
    fname = ftp.nlst()[0]
    get_binary(fname, src_path('tigerline', 'COUNTY', '2012', fname), ftp)
