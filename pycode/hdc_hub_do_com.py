import sas7bdat as sas
import pandas as pd
import os
import time
import sys
import logging



# log setting 
root = logging.getLogger()
root.setLevel(logging.DEBUG)

formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

fileHandler = logging.FileHandler("{0}/{1}.log".format('static/','datacalberlog'))
fileHandler.setFormatter(formatter)
root.addHandler(fileHandler)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
handler.setFormatter(formatter)
root.addHandler(handler)
# setting end

# file = './data/hdc_raw_data.sas7bdat'
# dofile = './data/Standardized Do Format.csv'
# doblockpath = './data/Hub DO Completeness block list.csv'
#
# resetfile = './data/rename_test.csv'
# curfile = './data/rename_current.csv'
# result_file = './data/HDC_QPM_HUB_DO_COMPLETENESS@CALFILE@CALFILE.csv'
# ../bin/python /home/ruser/Hub_DO_Completeness/hdc_hub_do_stage/hdc_hub_do_stage/pycode/hdc_hub_do_com.py


"""read file"""
file = '/srv/shiny-server/data/hdc_raw_data.sas7bdat'
dofile = '/srv/shiny-server/data/Standardized Do Format.csv'
doblockpath = '/srv/shiny-server/data/Hub DO Completeness block list.csv'

"""result file"""
resetfile = '/home/ruser/Hub_DO_Completeness/hdc_hub_do_stage/hdc_hub_do_stage/data/rename_test.csv'
curfile = '/home/ruser/Hub_DO_Completeness/hdc_hub_do_stage/hdc_hub_do_stage/data/rename_current.csv'
result_file = '/home/ruser/Hub_DO_Completeness/hdc_hub_do_stage/hdc_hub_do_stage/data/HDC_QPM_HUB_DO_COMPLETENESS@CALFILE.csv'


def read_sas(file_path):
    pieces = []
    with sas.SAS7BDAT(file_path) as f:
        pieces.append(f.to_data_frame())
        data = pd.concat(pieces)
    # data = data[data['HUBSUPDO_HUB_PART_NUM'].str[0] != 'U']
    return data


def sg_week(data):
    start_data_2018 = pd.Timestamp(2017, 7, 1, 0)
    start_data_2019 = pd.Timestamp(2018, 6, 30, 0)
    data_week = []
    for i in data.HUBSUPDO_HUB_CREATION_DATE:
        if i < start_data_2019:
            week_delta = int((i - start_data_2018).total_seconds()//(7*24*60*60)+1)
            data_week.append(''.join(['2018-', str(week_delta)]))
        else:
            week_delta = int((i - start_data_2019).total_seconds()//(7*24*60*60)+1)
            data_week.append(''.join(['2019-', str(week_delta)]))
    data['week'] = data_week
    return data


def do_sup_reset(data, dodata):
    data['DO_split'] = data['HUBSUPDO_DO_NUM']
    data_for_merge = data[['HUBSUPDO_DO_NUM', 'DO_split']].copy()
    data_for_reset = data_for_merge.copy()
    data_for_merge['SUPPLIER_NAME'] = 'nan'
    for i in range(8, 1, -1):
        data_for_reset['split'] = data_for_reset['DO_split'].str.slice(0, i)
        data_for_merge_new = pd.merge(data_for_reset, dodata, how='left', on='split', suffixes=('_L', '_R'), sort=False)
        data_for_merge['SUPPLIER_NAME_NEW'] = data_for_merge_new['SUPPLIER_NAME']
        try:
            data_for_merge['SUPPLIER_NAME'] = data_for_merge['SUPPLIER_NAME'].astype(str) + '__' \
                                              + data_for_merge['SUPPLIER_NAME_NEW'].astype(str)
            data_for_merge['SUPPLIER_NAME'] = data_for_merge['SUPPLIER_NAME'].str.split('__')
            data_for_merge['SUPPLIER_NAME'] = data_for_merge['SUPPLIER_NAME'].apply(
                lambda x: x[0] if x[0] != 'nan' else x[1])
            data_for_merge = data_for_merge[['HUBSUPDO_DO_NUM', 'SUPPLIER_NAME']]
        except:
            pass
    data['sup_name'] = data_for_merge['SUPPLIER_NAME']
    data = data[data['HUBSUPDO_HUB_PART_NUM'].str[0] != 'U']
    return data

# a = data_for_merge[data_for_merge.SUPPLIER_NAME.isna()==False]
# b = a[a.SUPPLIER_NAME == 'INTERPLEX C/O AMTEK ENGRG BATAM']


def do_data(do_file):
    dodata = pd.read_csv(do_file)
    dodata = dodata.dropna(subset=['DO_Format'])
    for i in dodata['DO_Format'].index:
        dodata.loc[i, 'DO_Format'] = dodata['DO_Format'].str.split('[xX][xX]')[i][0]
    dodata = dodata.sort_values(by='DO_Format')
    dodata.index = range(len(dodata))
    dodata['split'] = dodata['DO_Format']
    dodata = dodata.drop_duplicates(subset='DO_Format')
    return dodata[['DO_Format', 'SUPPLIER_NAME', 'split']]


def data_block(data, doblock_path):
    doblock = pd.read_csv(doblock_path)
    for i in doblock.index:
        try:
            fieldname = str(doblock['FIELD_NAME'][i]).split(';')
            fieldvalue = str(doblock['VALUE'][i]).split(';')
            boda = pd.DataFrame(fieldvalue, index=fieldname)
            boda = boda.T
            data_bo = data.copy()
            for j in fieldname:
                data_bo = data_bo[data_bo[j].isin(boda[j]) == True]
            data = data[data.isin(data_bo)[fieldname[0]] == False]
        except:
            pass
    return data


def time_dif(data):
    data = data.dropna(subset=['HUBSUPDO_HUB_CREATION_DATE'])
    data1 = data.copy()
    data_td1 = time_dif_cal(data, 'HUBSUPDO_HUB_ETL_LOAD_DATE')
    data_td2 = time_dif_cal(data1, 'CCVALFCT_ETL_LOAD_DATE_2')
    return data_td1, data_td2


def time_dif_cal(data, subset_name):
    data_td = data.dropna(subset=[subset_name]).copy()
    data_td['time'] = (data_td['HUBSUPDO_HUB_CREATION_DATE'] - data_td[subset_name])
    data_td_cal = data_td.groupby(data['HUBSUPDO_DO_NUM']).agg({
        'time': lambda x: x.mean()
    })
    td1 = []
    for k in data_td_cal.index:
        td1.append(data_td_cal['time'][k].total_seconds() / (3600 * 24))
    data_td_cal['time'] = td1
    return data_td_cal


def data_cal(data):
    data_lot = data.copy()
    g1 = data_lot.groupby(['HUBSUPDO_DO_NUM']).size()
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
    # data['in'] = range(len(data))
    # data = data.set_index(data['in'])
    data['DO'] = data['HUBSUPDO_DO_NUM']
    return data


def ups_data(data, upsdata):
    
    pass    


def main():
    day = time.strftime('%Y-%m-%d', time.localtime(time.time()))
    print(day)
    a = time.time()
    data = read_sas(file)
    b = time.time()
    print(b - a)
    print('data load ready')
    dodata = do_data(dofile)
    try:
        os.remove(resetfile)
    except:
        pass
    data = sg_week(data)
    data = do_sup_reset(data, dodata)
    data.to_csv(resetfile)
    data_td1, data_td2 = time_dif(data)
    data = data_cal(data)
    data['timedif1'] = data_td1
    data['timedif2'] = data_td2
    data = data_block(data, doblockpath)
    try:
        os.remove(result_file)
    except:
        pass
    data.drop(['HUBSUPDO_HUB_PRODUCT_KEY'],axis=1,inplace=True)
    data.drop(['HUBSUPDO_PART_REV'],axis=1,inplace=True)
    data.drop(['HUBSUPDO_SUPPLIER_PRODUCT_KEY'],axis=1,inplace=True)
    data.to_csv(result_file)
    b = time.time()
    print(b - a)


if __name__ == '__main__':
    main()
