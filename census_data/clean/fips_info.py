import pandas as pd

from econtools import load_or_build

from util.env import src_path

if __name__ == "__main__":
    df = pd.read_csv(src_path('cbsa2fipsxw.csv'))
    df = df.drop(0, axis=0)
    df = df.drop(['metrodivisioncode'], axis=1)

    # Deal with metro/micro
    tmp = df['metropolitanmicropolitanstatis'].value_counts()
    assert tmp.sum() == df.shape[0]
    assert len(tmp) == 2
    df['micropolitan'] = (
        df['metropolitanmicropolitanstatis'] ==
        'Metropolitan Statistical Area')
    df = df.drop('metropolitanmicropolitanstatis', axis=1)

    df = df.rename(columns={
        'cbsacode': 'cbsa',
        'cbsatitle': 'cbsa_name',
    })

    for col in ('cbsa',):
        df[col] = df[col].astype(int)
