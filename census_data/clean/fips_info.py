import os
import urllib.request
import pandas as pd

from util.env import src_path


def load_fips_cbsa():
    # Read or download the src
    filepath = src_path('cbsa2fipsxw.csv')
    if not os.path.isfile(filepath):
        url = (r'https://www.nber.org/cbsa-csa-fips-county-crosswalk/'
               'cbsa2fipsxw.csv')
        urllib.request.urlretrieve(url, filepath)
    else:
        pass
    df = pd.read_csv(filepath)

    df = df.drop(0, axis=0)
    df = df.drop(['metrodivisioncode'], axis=1)

    # Deal with metro/micro
    df = _convert_binary(df,
                         'metropolitanmicropolitanstatis',
                         'Metropolitan Statistical Area',
                         'micropolitan')
    df = _convert_binary(df,
                         'centraloutlyingcounty',
                         'Outlying',
                         'outlying_county')

    df = df.rename(columns={
        'cbsacode': 'cbsa',
        'cbsatitle': 'cbsa_name',
        'countycountyequivalent': 'county_name',
        'statename': 'state_name',
        'fipsstatecode': 'state',
        'fipscountycode': 'county',
    })

    for col in ('cbsa', 'state', 'county'):
        df[col] = df[col].astype(int)

    df['fips'] = (df['state'].astype(str).str.zfill(2) +
                  df['county'].astype(str).str.zfill(3))

    return df

def _convert_binary(df, col, target, new_name):
    tmp = df[col].value_counts()
    assert tmp.sum() == df.shape[0]
    assert len(tmp) == 2
    df[new_name] = df[col] == target
    assert df[new_name].sum() == tmp[target]
    df = df.drop(col, axis=1)
    return df


if __name__ == "__main__":
    df = load_fips_cbsa()
