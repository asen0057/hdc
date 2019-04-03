import pandas as pd
import os
import time
import sys
import logging
import xlrd

# log setting
root = logging.getLogger()
root.setLevel(logging.DEBUG)

formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

fileHandler = logging.FileHandler(
    "{0}/{1}.log".format('data/', 'upslog'))
fileHandler.setFormatter(formatter)
root.addHandler(fileHandler)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
handler.setFormatter(formatter)
root.addHandler(handler)
# setting end


result_file = 'data/HDC_QPM_HUB_DO_COMPLETENESS@CALFILE.csv'
folder = 'data/ups/'


# excel_file = 'data/UPSWUX-SeagateShipmentReport_201810310700to201811010700.xls'
# upsfile = 'data/gl1.csv'

# data = pd.read_csv(result_file, low_momery=False)
# wb = xlrd.open_workbook(excel_file,logfile=open(os.devnull, 'w'))
# dataex=pd.read_excel(wb,engine='xlrd')


# medata = dataex[['ORDERDATE','LOTTABLE01']]
# medata.columns = ['ORDERDATE','HUBSUPDO_DO_NUM']
# medata = medata.drop_duplicates(subset='HUBSUPDO_DO_NUM')
# data1 = data[['HUBSUPDO_DO_NUM', 'ORDERDATE']]
# data['ORDERDATE']=''

# datam = pd.merge(data1, medata,how='left', on='HUBSUPDO_DO_NUM')

# try:
#     datam['ORDERDATE'] = datam['ORDERDATE_x'].astype(str) + '__' + datam['ORDERDATE_y'].astype(str)
#     datam['ORDERDATE'] = datam['ORDERDATE'].str.split('__')
#     datam['ORDERDATE'] = datam['ORDERDATE'].apply(lambda x: x[0] if x[0] != 'nan' else x[1])
#     datam = datam[['HUBSUPDO_DO_NUM', 'ORDERDATE']]
# except:
#     pass

def ups_test(data, medata):
    # medata = dataex[['ORDERDATE', 'LOTTABLE01']]
    # medata.columns = ['ORDERDATE', 'HUBSUPDO_DO_NUM']
    # medata = medata.drop_duplicates(subset='HUBSUPDO_DO_NUM')
    datam = pd.merge(data, medata, how='left',
                     on='HUBSUPDO_DO_NUM', suffixes=('', '_New'))
    # datetype=type(medata['ORDERDATE'][0])
    # try:
    #     datam['ORDERDATE'] = datam['ORDERDATE_x'].astype(str) + '__' + datam['ORDERDATE_y'].astype(str)
    #     datam['ORDERDATE'] = datam['ORDERDATE'].str.split('__')
    #     datam['ORDERDATE'] = datam['ORDERDATE'].apply(
    #         lambda x: max(x[0],x[1]))
    #     datam = datam[['HUBSUPDO_DO_NUM', 'ORDERDATE']]
    # except:
    #     pass

    datam.loc[datam['ORDERDATE'].isnull(),
              'ORDERDATE'] = datam[datam['ORDERDATE'].isnull()]['ORDERDATE_New']
    datam = datam.drop(columns=['ORDERDATE_New'])
    return datam


def td3cal(data):
    data1 = data[['HUBSUPDO_HUB_CREATION_DATE', 'ORDERDATE']]
    data1 = data1.dropna(subset=['ORDERDATE'])
    data1['td'] = data1['HUBSUPDO_HUB_CREATION_DATE'].astype('datetime64') - data1['ORDERDATE'].astype('datetime64')
    data1['td'] = data1['td'].apply(lambda x: x.total_seconds() / (3600 * 24))
    # data11 = data1.dropna(subset='timedif1')
    # data11['timedif1ups'] = data11['timedif1'] - data11['td']
    # data12 = data1.dropna(subset='timedif2')
    # data12['timedif2ups'] = data12['timedif2'] - data12['td']
    # data1['timedif1ups'] = data11['timedif1ups']
    # data1['timedif2ups'] = data12['timedif2ups']
    return data1['td']


# def tdups(data)
#     data = 


def for_all_file(result_file, folder):
    data = pd.read_csv(result_file, low_memory=False)
    data['ORDERDATE'] = pd.Series([])
    data['ORDERDATE'] = data['ORDERDATE'].astype('datetime64')
    filelist = os.listdir(folder)
    for index,i in enumerate(filelist):
        root.info('current file is %s', i)
        wb = xlrd.open_workbook(folder + i, logfile=open(os.devnull, 'w'))
        dataex = pd.read_excel(wb, engine='xlrd')
        # root.info('dataex.columns is %s', dataex.columns)
        medata = dataex[['ORDERDATE', 'LOTTABLE01']]
        medata.columns = ['ORDERDATE', 'HUBSUPDO_DO_NUM']
        medata = medata.drop_duplicates(subset='HUBSUPDO_DO_NUM')
        data = ups_test(data, medata)
        root.info('%s finish', i)
        # break
    data['timedif3'] = td3cal(data)
    data.to_csv('data/upstest2.csv')


if __name__ == '__main__':
    for_all_file(result_file, folder)
