import pandas as pd

from econtools import load_or_build, stata_merge

from census_data.util.env import data_path, src_path


@load_or_build(data_path('bg_demogs_{year}.p'))
def load_bgdata(year):

    assert year in (2000, 2005)
    if year == 2000:
        file1 = src_path('demographics', 'nhgis0020_ds152_2000_blck_grp.csv')
        file2 = src_path('demographics', 'nhgis0020_ds147_2000_blck_grp.csv')
        tmp1 = pd.read_csv(file1)
        tmp2 = pd.read_csv(file2)
        # Drop all duplicate columns except gisjoin
        cols1 = set(tmp1.columns)
        cols2 = set(tmp2.columns)
        keep_set = cols1.symmetric_difference(cols2)
        bg = ['STATEA', 'COUNTYA', 'TRACTA', 'BLCK_GRPA']
        keep_cols1 = [x for x in tmp1.columns if x in keep_set]
        keep_cols2 = [x for x in tmp2.columns if x in keep_set]
        tmp1 = tmp1[['GISJOIN'] + bg + keep_cols1]
        tmp2 = tmp2[['GISJOIN'] + keep_cols2]
        rawdata = stata_merge(tmp1, tmp2, on='GISJOIN', assertval=3)
    else:
        nhgis_path = src_path('demographics',
                              'nhgis0017_ds195_20095_2009_blck_grp.csv')
        rawdata = pd.read_csv(nhgis_path)

    # Rename useful variables
    new_names = _rename_dict(year)
    rawdata.rename(columns=new_names, inplace=True)

    # Restrict columns
    rawdata.rename(columns=lambda x: x.lower(), inplace=True)
    if year == 2000:
        rawdata.rename(columns={'blck_grpa': 'blkgrpa'}, inplace=True)
        bg = ['statea', 'countya', 'tracta', 'blkgrpa']
        data = rawdata[bg + list(new_names.values())].copy()
    else:
        bg = ['statea', 'countya', 'tracta', 'blkgrpa']
        data = rawdata[bg + list(new_names.values())].copy()

    # Fix weird stuff in data
    _fix_errata(data, year)

    data['bg'] = data.apply(_gen_bgid, axis=1)
    data.drop(bg, axis=1, inplace=True)
    data['year'] = year
    data.set_index('bg', inplace=True)
    return data

def _rename_dict(year):
    if year == 2000:
        new_names = {
            'FXS001': 'pop',
            'FY4001': 'households',
            # Race (exclusive groups)
            'FX1001': 'race_white',
            'FX1002': 'race_black',
            'FXZ001': 'race_hisp',
            # Housing
            'FV5001': 'hunit',
            'FV8002': 'hunit_vacant',
            'FWA001': 'hunit_owner',
            'FWA002': 'hunit_renter',
            'G8V001': 'hvalue_median',
            'G8C001': 'rent_median',    # contract rent + tenant-paid utilities
            'G74001': 'crent_median',   # contract rent
            # Income
            'HF6001': 'hhincmed',
            'HG4001': 'incpercap',
            'HF5001': 'hhinc_10',
            'HF5002': 'hhinc_15',
            'HF5003': 'hhinc_20',
            'HF5004': 'hhinc_25',
            'HF5005': 'hhinc_30',
            'HF5006': 'hhinc_35',
            'HF5007': 'hhinc_40',
            'HF5008': 'hhinc_45',
            'HF5009': 'hhinc_50',
            'HF5010': 'hhinc_60',
            'HF5011': 'hhinc_75',
            'HF5012': 'hhinc_100',
            'HF5013': 'hhinc_125',
            'HF5014': 'hhinc_150',
            'HF5015': 'hhinc_200',
            'HF5016': 'hhinc_201',
            # Educ (age > 25) by sex
            'HD1001': 'male_ed0',
            'HD1002': 'male_ed4',
            'HD1003': 'male_ed6',
            'HD1004': 'male_ed8',
            'HD1005': 'male_ed9',
            'HD1006': 'male_ed10',
            'HD1007': 'male_ed11',
            'HD1008': 'male_ed12',
            'HD1009': 'male_ed_hs',
            'HD1010': 'male_ed_coll_1',
            'HD1011': 'male_ed_coll_nod',
            'HD1012': 'male_ed_aa',
            'HD1013': 'male_ed_ba',
            'HD1014': 'male_ed_ma',
            'HD1015': 'male_ed_jd',
            'HD1016': 'male_ed_phd',
            'HD1017': 'female_ed0',
            'HD1018': 'female_ed4',
            'HD1019': 'female_ed6',
            'HD1020': 'female_ed8',
            'HD1021': 'female_ed9',
            'HD1022': 'female_ed10',
            'HD1023': 'female_ed11',
            'HD1024': 'female_ed12',
            'HD1025': 'female_ed_hs',
            'HD1026': 'female_ed_coll_1',
            'HD1027': 'female_ed_coll_nod',
            'HD1028': 'female_ed_aa',
            'HD1029': 'female_ed_ba',
            'HD1030': 'female_ed_ma',
            'HD1031': 'female_ed_jd',
            'HD1032': 'female_ed_phd',
        }
    else:
        new_names = {
            'RK9E001': 'pop',
            'RNGE001': 'households',
            # 'RKYE003': 'female',              # From population var
            # 'RM8E019': 'female',              # From education var
            # 'RM8E002': 'male',
            # Race (exclusive groups)
            'RLIE003': 'race_white',
            'RLIE004': 'race_black',
            'RLIE012': 'race_hisp',
            # Housing
            'RQJE001': 'hunit_vacant',
            'RQJE002': 'hunit_vacant_rent',
            'RQJE004': 'hunit_vacant_sale',
            'RP9E001': 'hunit',
            'RP9E002': 'hunit_owner',
            'RP9E003': 'hunit_renter',
            'RRUE001': 'rent_median',
            'RROE001': 'crent_median',
            # Household income
            'RNHE001': 'hhincmed',
            'RNGE002': 'hhinc_10',
            'RNGE003': 'hhinc_15',
            'RNGE004': 'hhinc_20',
            'RNGE005': 'hhinc_25',
            'RNGE006': 'hhinc_30',
            'RNGE007': 'hhinc_35',
            'RNGE008': 'hhinc_40',
            'RNGE009': 'hhinc_45',
            'RNGE010': 'hhinc_50',
            'RNGE011': 'hhinc_60',
            'RNGE012': 'hhinc_75',
            'RNGE013': 'hhinc_100',
            'RNGE014': 'hhinc_125',
            'RNGE015': 'hhinc_150',
            'RNGE016': 'hhinc_200',
            'RNGE017': 'hhinc_201',
            # Educ (age > 25) by sex
            'RM8E003': 'male_ed0',
            'RM8E004': 'male_ed4',
            'RM8E005': 'male_ed6',
            'RM8E006': 'male_ed8',
            'RM8E007': 'male_ed9',
            'RM8E008': 'male_ed10',
            'RM8E009': 'male_ed11',
            'RM8E010': 'male_ed12',
            'RM8E011': 'male_ed_hs',
            'RM8E012': 'male_ed_coll_1',
            'RM8E013': 'male_ed_coll_nod',
            'RM8E014': 'male_ed_aa',
            'RM8E015': 'male_ed_ba',
            'RM8E016': 'male_ed_ma',
            'RM8E017': 'male_ed_jd',
            'RM8E018': 'male_ed_phd',
            'RM8E020': 'female_ed0',
            'RM8E021': 'female_ed4',
            'RM8E022': 'female_ed6',
            'RM8E023': 'female_ed8',
            'RM8E024': 'female_ed9',
            'RM8E025': 'female_ed10',
            'RM8E026': 'female_ed11',
            'RM8E027': 'female_ed12',
            'RM8E028': 'female_ed_hs',
            'RM8E029': 'female_ed_coll_1',
            'RM8E030': 'female_ed_coll_nod',
            'RM8E031': 'female_ed_aa',
            'RM8E032': 'female_ed_ba',
            'RM8E033': 'female_ed_ma',
            'RM8E034': 'female_ed_jd',
            'RM8E035': 'female_ed_phd',
        }
    return new_names

def _gen_bgid(x):
    state = str(int(x['statea'])).zfill(2)
    county = str(int(x['countya'])).zfill(3)
    tract = str(int(x['tracta'])).zfill(6)
    bg = int(x['blkgrpa'])
    block_group_id = '{}{}{}{}'.format(state, county, tract, bg)
    return block_group_id

def _fix_errata(df, year):
    """ Should modify `df` in place. """
    if year == 2005:
        df['hunit'] = df['households'] + df['hunit_vacant']


if __name__ == '__main__':
    df = load_bgdata(2000, _rebuild=True)
    # df2 = load_bgdata(2005, _rebuild=True)
