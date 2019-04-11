import pandas as pd

from census_data.util.env import src_path


def block_pop_2000():
    """ Year 2000 pop, block-level """

    nhgis_path = src_path('demographics', 'nhgis0009_ds147_2000_block.csv')

    usecols = ['STATEA', 'COUNTYA', 'TRACTA', 'BLOCKA', 'FXS001']
    df = pd.read_csv(nhgis_path, usecols=usecols, header=0)

    df = df.rename(columns={'FXS001': 'pop2000'})

    df = df.rename(columns=lambda x: x.lower())

    # Restrict to fips_list
    df['fips'] = (df['statea'].astype(str).str.zfill(2)
                  + df['countya'].astype(str).str.zfill(3))
    df['blockID'] = (df['fips']
                     + df['tracta'].astype(str).str.zfill(6)
                     + df['blocka'].astype(str).str.zfill(4))

    df = df.drop(['statea', 'countya', 'tracta', 'blocka'])

    df = df.set_index('blockID')

    return df


if __name__ == "__main__":
    pass
