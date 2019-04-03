import sas7bdat as sas
import pandas as pd
import os
import re
import time

file = './data/hdc_raw_data.sas7bdat'
dofile = './data/Standardized Do Format.xlsx'
resetfile = './data/rename_test.csv'
curfile = './data/rename_current.csv'


def readsas(file_path):
    pieces = []
    with sas.SAS7BDAT(file_path) as f:
        pieces.append(f.to_data_frame())
        data = pd.concat(pieces)
    return data


def sg_week(data):
    start_data_2018 = pd.Timestamp(2017, 7, 1, 0)
    start_data_2019 = pd.Timestamp(2018, 7, 1, 0)
    data_week = []
    for i in data.HUBSUPDO_HUB_CREATION_DATE:
        if i < start_data_2019:
            week_delta = ((i - start_data_2018).days + 1) // 7
            data_week.append(''.join(['2018-', str(week_delta)]))
        else:
            week_delta = ((i - start_data_2019).days + 1) // 7
            data_week.append(''.join(['2019-', str(week_delta)]))
    data['week'] = data_week
    return data


# def do_sup_reset(data, dodata):
#     dodata = dodata.dropna(subset=['DO_Format'])
#     for i in dodata['DO_Format'].index:
#         dodata.loc[i, 'DO_Format'] = dodata['DO_Format'].str.split('[xX][xX]')[i][0]
#     dodata = dodata.sort_values(by='DO_Format')
#     dodata.index = range(len(dodata))
#     supname = []
#     inlist = []
#     for j in data['HUBSUPDO_DO_NUM'].index:
#         sn = ''
#         for i in dodata['DO_Format'].index:
#             pattern_do = re.compile(r'^' + dodata.loc[i, 'DO_Format'] + '.*')
#             fi = re.findall(pattern_do, str(data['HUBSUPDO_DO_NUM'][j]))
#             if fi != []:
#                 sn = dodata['SUPPLIER_NAME'][i]
#         if sn != '':
#             supname.append(sn)
#             inlist.append(j)
#             print(i, j)
#     sup = pd.DataFrame({'sup_name': supname, 'in': inlist})
#     data = pd.merge(data, sup, how='left', on='in')
#     return data

# def do_sup_reset(data, dodata):
#     dodata = dodata.dropna(subset=['DO_Format'])
#     for i in dodata['DO_Format'].index:
#         dodata.loc[i, 'DO_Format'] = dodata['DO_Format'].str.split('[xX][xX]')[i][0]
#     dodata = dodata.sort_values(by='DO_Format')
#     dodata.index = range(len(dodata))
#     data[('do3')] = data['HUBSUPDO_DO_NUM'].str.slice(0, 3)
#     data[('do4')] = data['HUBSUPDO_DO_NUM'].str.slice(0, 4)
#     data[('do5')] = data['HUBSUPDO_DO_NUM'].str.slice(0, 5)
#     data[('do6')] = data['HUBSUPDO_DO_NUM'].str.slice(0, 6)
#     dodata['do3'] = dodata['DO_Format']
#     dodata['do4'] = dodata['DO_Format']
#     dodata['do5'] = dodata['DO_Format']
#     dodata['do6'] = dodata['DO_Format']
#     dodata3 = dodata[['do3', 'SUPPLIER_NAME']]
#     data = pd.merge(data, dodata3, how='left', on='do3')
#     dodata4 = dodata[['do4', 'SUPPLIER_NAME']]
#     data = pd.merge(data, dodata4, how='left', on='do4')
#     dodata5 = dodata[['do5', 'SUPPLIER_NAME']]
#     data = pd.merge(data, dodata5, how='left', on='do5')
#     dodata6 = dodata[['do6', 'SUPPLIER_NAME']]
#     data = pd.merge(data, dodata6, how='left', on='do6')

# def do_sup_reset(data,dodata):
#     data['DO_split'] = data['HUBSUPDO_DO_NUM']
#     data_for_merge = data[['HUBSUPDO_DO_NUM', 'DO_split']].copy()
#     data_for_reset = data_for_merge.copy()
#     for i in range(7, 3, -1):
#         data_for_reset['split'] = data_for_reset['DO_split'].str.slice(0, i)
#         data_for_merge_new = pd.merge(data_for_reset, dodata,how='left', on='split', suffixes=('_L', '_R'), sort=False)
#         # data_for_merge_new = data_for_merge_new[['HUBSUPDO_DO_NUM', 'SUPPLIER_NAME']]
#         data_for_merge = pd.merge(data_for_merge, data_for_merge_new, on='HUBSUPDO_DO_NUM',
#                                   suffixes=('_L', '_R'), sort=False)
#         try:
#             data_for_merge['SUPPLIER_NAME'] = data_for_merge['SUPPLIER_NAME_L'].astype(str) + data_for_merge['SUPPLIER_NAME_R'].astype(str)
#             data_for_merge = data_for_merge[['HUBSUPDO_DO_NUM', 'SUPPLIER_NAME']]
#             data_for_merge['SUPPLIER_NAME'] = data_for_merge['SUPPLIER_NAME'].str.replace('nan', '')
#         except:
#             pass
#         data_for_reset = data_for_merge_new[data_for_merge_new['SUPPLIER_NAME'].isnull()]
#         data_for_reset = data_for_reset[['HUBSUPDO_DO_NUM', 'DO_split']]
#     data = pd.merge(data, data_for_merge, on='HUBSUPDO_DO_NUM', sort=False)
#     return data

def do_sup_reset(data, dodata):
    data['DO_split'] = data['HUBSUPDO_DO_NUM']
    data_for_merge = data[['HUBSUPDO_DO_NUM', 'DO_split']].copy()
    # data_for_merge['in'] = range(len(data_for_merge))
    data_for_reset = data_for_merge.copy()
    data_for_merge['SUPPLIER_NAME'] = 'nan'
    for i in range(7, 2, -1):
        data_for_reset['split'] = data_for_reset['DO_split'].str.slice(0, i)
        data_for_merge_new = pd.merge(data_for_reset, dodata, how='left', on='split', suffixes=('_L', '_R'), sort=False)
        data_for_merge['SUPPLIER_NAME_NEW'] = data_for_merge_new['SUPPLIER_NAME']
        # data_for_merge = pd.merge(data_for_merge, data_for_merge_new[['HUBSUPDO_DO_NUM','SUPPLIER_NAME']], how='left', on='HUBSUPDO_DO_NUM',suffixes=('', '_NEW'), sort=False)
        try:
            data_for_merge['SUPPLIER_NAME'] = data_for_merge['SUPPLIER_NAME'].astype(str) + data_for_merge[
                'SUPPLIER_NAME_NEW'].astype(str)
            data_for_merge = data_for_merge[['HUBSUPDO_DO_NUM', 'SUPPLIER_NAME']]
            data_for_merge['SUPPLIER_NAME'] = data_for_merge['SUPPLIER_NAME'].str.replace('nan', '')
        except:
            pass
        data_for_reset = data_for_merge_new[data_for_merge_new['SUPPLIER_NAME'].isnull()]
        data_for_reset = data_for_reset[['HUBSUPDO_DO_NUM', 'DO_split']]
    data['sup_name'] = data_for_merge['SUPPLIER_NAME']
    return data


def get_last_metch(a, b):
    if a == 'nan':
        a = b
    return a


def do_sup_reset(data, dodata):
    data['DO_split'] = data['HUBSUPDO_DO_NUM']
    data_for_merge = data[['HUBSUPDO_DO_NUM', 'DO_split']].copy()
    data_for_reset = data_for_merge.copy()
    data_for_merge['SUPPLIER_NAME'] = 'nan'
    for i in range(8, 1, -1):
        data_for_reset['split'] = data_for_reset['DO_split'].str.slice(0, i)
        data_for_merge_new = pd.merge(data_for_reset, dodata, how='left', on='split', suffixes=('_L', '_R'), sort=False)
        data_for_merge['SUPPLIER_NAME_NEW'] = data_for_merge_new['SUPPLIER_NAME']
        print(len(data_for_merge_new))
        try:
            data_for_merge['SUPPLIER_NAME'] = data_for_merge['SUPPLIER_NAME'].astype(str)+',' \
                                              + data_for_merge['SUPPLIER_NAME_NEW'].astype(str)
            data_for_merge['SUPPLIER_NAME'] = data_for_merge['SUPPLIER_NAME'].str.split(',')
            data_for_merge['SUPPLIER_NAME'] = data_for_merge['SUPPLIER_NAME'].apply(
                lambda x: x[0] if x[0] != 'nan' else x[1])
            data_for_merge = data_for_merge[['HUBSUPDO_DO_NUM', 'SUPPLIER_NAME']]
            data_for_merge['SUPPLIER_NAME'] = data_for_merge['SUPPLIER_NAME'].str.replace('nan', '')
        except:
            pass
        try:

            data_for_merge['SUPPLIER_NAME'] = data_for_merge['SUPPLIER_NAME', 'SUPPLIER_NAME_NEW'].apply(
                lambda x, y: x if x != 'nan' else y)
        except:
            pass
    # data_for_merge['sup_name'] = data_for_merge['sup_name'].str.replace(
    #     'MMI PRECISION (THAILAND)MMI INDUSTRIES (WUXI) CO LTD',
    #     'MMI PRECISION (THAILAND)')
    # data_for_merge['sup_name'] = data_for_merge['sup_name'].str.replace(
    #     'SHIN-ETSU MALAYSIA (SHAM ALAM)SHIN-ETSU MAGNETICS (THAI) LTD',
    #     'SHIN-ETSU MALAYSIA (SHAM ALAM)')
    data['sup_name'] = data_for_merge['SUPPLIER_NAME']
    print(data['sup_name'].unique())
    return data

data_for_merge['SUPPLIER_NAME_NEW'] = data_for_merge['SUPPLIER_NAME'].apply(lambda x: x if x !='nan')
'MMI PRECISION (THAILAND)MMI INDUSTRIES (WUXI) CO LTD'
'SHIN-ETSU MALAYSIA (SHAM ALAM)SHIN-ETSU MAGNETICS (THAI) LTD'


def do_data(dofile):
    dodata = pd.read_excel(dofile, sheet_name=0)
    dodata = dodata.dropna(subset=['DO_Format'])
    for i in dodata['DO_Format'].index:
        dodata.loc[i, 'DO_Format'] = dodata['DO_Format'].str.split('[xX][xX]')[i][0]
    dodata = dodata.sort_values(by='DO_Format')
    dodata.index = range(len(dodata))
    dodata['split'] = dodata['DO_Format']
    dodata = dodata.drop_duplicates(subset='DO_Format')
    return dodata[['DO_Format', 'SUPPLIER_NAME', 'split']]


def timedif(data):
    data = data.dropna(subset=['HUBSUPDO_HUB_CREATION_DATE'])
    data1 = data.copy()
    datatd1 = timedifcal(data, 'HUBSUPDO_HUB_ETL_LOAD_DATE')
    datatd2 = timedifcal(data1, 'CCVALFCT_ETL_LOAD_DATE_2')
    return datatd1, datatd2


def timedifcal(data, subsetname):
    datatd = data.dropna(subset=[subsetname]).copy()
    datatd['time'] = (datatd['HUBSUPDO_HUB_CREATION_DATE'] - datatd[subsetname])
    datatdcal = datatd.groupby(data['HUBSUPDO_DO_NUM']).agg({
        'time': lambda x: x.mean()
    })
    td1 = []
    for k in datatdcal.index:
        td1.append(datatdcal['time'][k].total_seconds() / (3600 * 24))
    datatdcal['time'] = td1
    return datatdcal


def data_cal(data):
    data_lot = data.copy()
    g1 = data_lot.groupby(['HUBSUPDO_DO_NUM']).size()
    # data_lot = data_lot.fillna(1)
    data_gro = data_lot.copy()
    data_lot = data_lot[data_lot['HUBSUPDO_LOT_NUM'] != '']
    data_gro = data_gro[data_gro.CCVALFCT_GROUP_NUM_2 == '']
    data = data.dropna(subset=['HUBSUPDO_HUB_CREATION_DATE'])

    data = data.drop_duplicates(['HUBSUPDO_DO_NUM'])
    data_lot = data_lot.drop_duplicates(['HUBSUPDO_DO_NUM'])
    data_gro = data_gro.drop_duplicates(['HUBSUPDO_DO_NUM'])

    data = data.set_index('HUBSUPDO_DO_NUM')
    data_lot = data_lot.set_index('HUBSUPDO_DO_NUM')
    data_gro = data_gro.set_index('HUBSUPDO_DO_NUM')

    data['lotcal_true'] = data_lot['HUBSUPDO_LOT_NUM']
    data_relot = data.copy()
    data_relot = data_relot.fillna(1)
    data_relot = data_relot[data_relot['lotcal_true'] == 1]
    data_gro['CCVALFCT_GROUP_NUM_2'] = 1
    data['lotcal'] = data_relot['lotcal_true']
    data['grocal'] = data_gro['CCVALFCT_GROUP_NUM_2']
    data['size'] = g1
    data['HUBSUPDO_DO_NUM'] = data.index
    data['in'] = range(len(data))
    data = data.set_index(data['in'])
    data['DO'] = data['HUBSUPDO_DO_NUM']
    return data


def reset_main():
    dodata = do_data(dofile)
    try:
        os.remove(resetfile)
    except:
        pass
    # n = 0
    for chunk in pd.read_sas(file, chunksize=100000, encoding='utf-8'):
        data = sg_week(chunk)
        data = do_sup_reset(data, dodata)
        data.to_csv(curfile)
        with open(resetfile, 'ab') as f:
            f.write(open(curfile, 'rb').read())


def main():
    a = time.time()
    data = readsas(file)
    dodata = do_data(dofile)
    try:
        os.remove(resetfile)
    except:
        pass
    data = sg_week(data)
    data = do_sup_reset(data, dodata)
    data.to_csv(resetfile)
    datatd1, datatd2 = timedif(data)
    data = data_cal(data)
    data['timedif1'] = datatd1
    data['timedif2'] = datatd2
    dodata = do_data(dofile)
    data = sg_week(data)
    data = do_sup_reset(data, dodata)
    data.to_csv('./data/HDC_QPM_HUB_DO_COMPLETENESS@CALFILE.csv')
    b = time.time()
    print(b - a)


if __name__ == '__main__':
    main()
