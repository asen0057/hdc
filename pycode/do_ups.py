import pandas as pd 
import sas7bdat as sas 
import re,os, time, sys, logging, xlrd


# log setting
root = logging.getLogger()
root.setLevel(logging.DEBUG)

formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

fileHandler = logging.FileHandler(
    "{0}/{1}.log".format('data/', 'do_ups_log'))
fileHandler.setFormatter(formatter)
root.addHandler(fileHandler)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
handler.setFormatter(formatter)
root.addHandler(handler)
# setting end


hdc_file = './data/hdc_raw_data.sas7bdat'
doFile = './data/Standardized Do Format.csv'
doBlockPath = './data/Hub DO Completeness block list.csv'

resetFile = './data/ups_reset_name.csv'
curFile = './data/rename_current.csv'
resultFile = './data/UPS_QPM_HUB_DO_COMPLETENESS@CALFILE@CALFILE.csv'
upsFolder = './data/ups/'

def read_sas(filePath: 'string') -> 'dataFrame':
    pieces = []
    with sas.SAS7BDAT(filePath) as f:
        pieces.append(f.to_data_frame())
        data = pd.concat(pieces)
    return data


def read_do(doFile: 'string') -> 'dict':
    doData = pd.read_csv(doFile) 
    doData = doData.dropna(subset=['DO_Format'])
    doData.index = doData.DO_Format.apply(lambda x: re.split('[xX][xX]', x)[0])
    doData = doData.sort_index()
    mapData = doData.SUPPLIER_NAME.to_dict()
    return mapData

def read_ups(folder):
    fileList = os.listdir(folder)
    pieces = [] 
    for index, file in enumerate(fileList):
        root.info('current file i %s', file)
        wb = xlrd.open_workbook(folder + file, logfile=open(os.devnull, 'w'))
        dataPieces = pd.read_excel(wb, engine='xlrd')
        dataPieces = dataPieces[['ORDERDATE', 'LOTTABLE01']]
        dataPieces.columns = ['ORDERDATE', 'HUBSUPDO_DO_NUM']
        pieces.append(dataPieces)
        root.info('data piece add done %s', index)
    dataUps =  pd.concat(pieces)
    dataUps = dataUps.drop_duplicates(subset='HUBSUPDO_DO_NUM')
    return dataUps


def do_sup_reset(data, supMap):
    data['SUPPLIER_NAME'] = 'na'
    for key in supMap.keys():
        data['SUPPLIER_NAME'] = data.apply(lambda row: supMap[key] if re.match(key,row['HUBSUPDO_DO_NUM']) != None else row['SUPPLIER_NAME'], axis=1)
        root.info('replace suplier name %s to %s', key, supMap[key])
    data = sg_week(data)
    return data

def sg_week(data):
    start_data_2019 = pd.Timestamp(2018, 6, 30, 0)
    data['week'] = data.HUBSUPDO_HUB_CREATION_DATE.apply(
        lambda x: ('-').join(['2019', str(int((x-start_data_2019).total_seconds()//(7*24*60*60))+1)]))
    return data


# supMap = read_do(doFile)
# data = read_sas(hdc_file)
# data = do_sup_reset(data, supMap)
# data.to_csv(resetFile)
# dataUps = read_ups(upsFolder)
# dataMerge = pd.merge(data, dataUps,how='left', on='HUBSUPDO_DO_NUM')
# dataMerge['HUBSUPDO_HUB_CREATION_DATE'] = dataMerge['ORDERDATE'].astype('datetime64')
# dataMerge = dataMerge.dropna(subset=['ORDERDATE'])
# data_td1,data_td2 = time_dif(dataMerge)
# data = data_cal(dataMerge)
# data['timedif1'] = data_td1
# data['timedif2'] = data_td2
# # data = data_block(data, doblockpath)
# try:
#     os.remove(result_file)
# except:
#     pass
# data.drop(['HUBSUPDO_HUB_PRODUCT_KEY'],axis=1,inplace=True)
# data.drop(['HUBSUPDO_PART_REV'],axis=1,inplace=True)
# data.drop(['HUBSUPDO_SUPPLIER_PRODUCT_KEY'],axis=1,inplace=True)
# data.to_csv(resultFile)


def time_dif(data):
    data = data.dropna(subset=['HUBSUPDO_HUB_CREATION_DATE'])
    data1 = data.copy()
    data_td1 = time_dif_cal(data, 'HUBSUPDO_HUB_ETL_LOAD_DATE')
    data_td2 = time_dif_cal(data1, 'CCVALFCT_ETL_LOAD_DATE_2')
    return data_td1, data_td2


def time_dif_cal(data, subset_name):
    data_td = data.dropna(subset=[subset_name]).copy()
    data_td['time'] = data_td['HUBSUPDO_HUB_CREATION_DATE'] - data_td[subset_name].astype('datetime64')
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
    # data = data.dropna(subset=['HUBSUPDO_HUB_CREATION_DATE'])

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
    data['DO'] = data['HUBSUPDO_DO_NUM']
    return data

def main():
    supMap = read_do(doFile)
    data = read_sas(hdc_file)
    data = do_sup_reset(data, supMap)
    data.to_csv(resetFile)
    dataUps = read_ups(upsFolder)
    dataMerge = pd.merge(data, dataUps,how='left', on='HUBSUPDO_DO_NUM')
    dataMerge['HUBSUPDO_HUB_CREATION_DATE'] = dataMerge['ORDERDATE']
    # dataMerge = dataMerge.dropna(subset=['ORDERDATE'])
    data_td1,data_td2 = time_dif(dataMerge)
    data = data_cal(dataMerge)
    data['timedif1'] = data_td1
    data['timedif2'] = data_td2
    # data = data_block(data, doblockpath)
    try:
        os.remove(result_file)
    except:
        pass
    data.drop(['HUBSUPDO_HUB_PRODUCT_KEY'],axis=1,inplace=True)
    data.drop(['HUBSUPDO_PART_REV'],axis=1,inplace=True)
    data.drop(['HUBSUPDO_SUPPLIER_PRODUCT_KEY'],axis=1,inplace=True)
    data.to_csv(resultFile)

if __name__ == '__main__':
    main()