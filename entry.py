# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import os
import time
import threading
from multiprocessing.pool import Pool
from datetime import *
from decimal import Decimal
g_assert = False
strYMD = ''
def initialDueConfig(file):
    global g_assert
    # assert not _test or x > 0
    # assert self.clnt.stop_io()==1, "IO stop failed"
    assert not g_assert or os.path.isfile(file)
    ans = {}
    with open(file, 'r') as srcFile:
        textlist = srcFile.readlines()
    for eachline in textlist:
        eachline = eachline.strip()
        if not eachline:
            continue
        # eachline = eachline.strip()
        splitLine = eachline.split('=')
        assert not g_assert or len(splitLine) >= 2
        key = splitLine[0]

        ans[key] = splitLine[1]
    return ans
def initialProductConfig(file):

    global g_assert


    assert not g_assert or os.path.isfile(file)
    ans = {}
    with open(file, 'r') as srcFile:
        textlist = srcFile.readlines()
    numSet = ['year', 'month']

    for eachline in textlist:
        eachline = eachline.strip()
        splitLine = eachline.split('=')
        assert not g_assert or len(splitLine) >= 2
        key = splitLine[0]
        if key in numSet:
            mval = Decimal(splitLine[1])
        elif ',' in splitLine[1]:
            splitLine[1].strip()
            # temp = line.strip()
            funds = splitLine[1].split(',')
            trimFunds = []
            for eachFund in funds:
                #eachFund.replace(' ', '')
                trimFunds.append(eachFund.strip())
            mval = trimFunds
        else:
            mval = splitLine[1].strip()




        ans[key] = mval

    return ans
def readAll(path, fileType):
    files = os.listdir(path)
    textContainer = []
    excelFileList = []
    for file in files:
        if 'Positions' not in file:
            continue
        file = path + '/' + file
        if not os.path.isfile(file):
            continue
        if file.endswith(fileType):
            excelFileList.append(file)
            print(file)


    return excelFileList

def filter(date, path = "./csv"):
    global strYMD
    print(strYMD)
    # dueConfigPath = "./config/date.txt"
    # g_dueFunds = initialDueConfig(dueConfigPath)
    excelFileList = readAll(path, 'csv')
    fileLen = len(excelFileList)
    dateList = [date for x in range(fileLen)]
    argList = [(tpath, tday) for tpath in excelFileList for tday in dateList ]
    with Pool(4) as p:
        p.map(changeDate, argList)
    return
def replaceDate(row):
    row["OpenDate"] = "test"
    return
def changeDate(arg):
    path, cday = arg
    print(path)
    dirsplit = os.path.split(path)
    name = dirsplit[-1]
    outputPath = "./output/" + name
    last_row = None
    try:
        # skip_footer=1, engine='python'
        df = pd.read_csv(path, header=1)
        last_row = df[-1]
        df = df[:-1]
        # df_2b = pd.read_csv(path, header=None, skiprows=[0])
    except Exception as e:
        print(e)
    # print(df_2b)
    # # df.ix[1:3, [1, 2]]
    # df_2b.ix[1:, 0] = df_2b.ix[1:, 0].apply(lambda x: cday  )
    # df_2b = df_2b.fillna("")
    # print(df_2b)
    print(df["OpenDate"])
    print(df.head())
    # print(df.head)
    # df.apply(lambda x: np.nan if pd.isnull(x.Upper) \
    #     else 'U' if x.Price > x.Upper
    # else 'D' if x.Price < x.Lower \
    #     else 'M', axis=1)
    df["OpenDate"] =df["OpenDate"].apply(lambda x: cday)
    print(df["OpenDate"])

    arr = df.values
    df = df.fillna("N/A")
    # ds = df.iloc[[1,17],[0,2]]
    ds = df
    # ds = df.ix[:, 0:17]
    print(ds)
    # ds.to_csv(outputPath, index=False)
    # OpenDate	Serial_No	Exchange	Product Code	L-Lots	BidPrice	S-Lots	AskPrice
    # Previous_SP	SP	Float_P/L	MTM_P/L	H/S	Margin	Contract Size	Account Code	Expiry Year

    # df['h-index'] = df.groupby('author')['citations'].transform(lambda x: ( x >= x.rank(ascending=False, method='first') ).sum() )
    # df['h-index'] = df.groupby('author')['citations'].transform(lambda x: ( x >= x.rank(ascending=False, method='first') ).sum() ) ​
    # dsg = ds.groupby(['OpenDate', 'Product Code', 'L-Lots', 'BidPrice', 'S-Lots', 'AskPrice', 'H/S'], as_index=False).agg({'Margin': np.sum, 'Account Code': np.mean})
    gb_cols = ['OpenDate', 'Product Code', 'L-Lots', 'BidPrice', 'S-Lots', 'AskPrice', 'H/S']
    dsg = ds.groupby(gb_cols, as_index=False)['Margin', 'Float_P/L', 'MTM_P/L'].sum()
    dsgf = ds.groupby(gb_cols, as_index=False).first()
    dsgf = dsgf.drop(['Margin', 'Float_P/L', 'MTM_P/L'], axis=1)
    result = pd.merge(dsg, dsgf, how='left', on=gb_cols)
    print(result)
    # ignore_index=True
    result.append(last_row)
    # result = pd.concat([result, last_row])
    result = df.sort(['Serial_No', 'Exchange'], ascending=[1, 1])
    print(result)
    #
    #
    #
    col_seq = ['OpenDate', 'Serial_No', 'Exchange', 'Product Code', 'L-Lots', 'BidPrice', 'S-Lots',
               'AskPrice', 'Previous_SP', 'SP', 'Float_P/L', 'MTM_P/L', 'H/S', 'Margin', 'Contract Size',
               'Account Code', 'Expiry Year', 'Expiry Month']
    result.to_csv(outputPath, index=False, columns=col_seq)
    # agg({'Margin': ['sum'], 'Account Code': ['mean']})
    # print(dsg)
    # print("-" * 100)
    # dsg = ds.groupby(['Product Code', 'L-Lots', 'BidPrice', 'S-Lots', 'AskPrice', 'H/S'])['Margin', 'Float_P/L', 'MTM_P/L'].sum()
    # # agg({'Margin': ['sum'], 'Account Code': ['mean']})
    # print(dsg)
    # print("-"*100)
    # dsg = ds.groupby(['Product Code', 'L-Lots', 'BidPrice', 'S-Lots', 'AskPrice', 'H/S'])['Account Code'].first()
    # print(dsg)
    # print("-" * 100)
    # dsg = ds.groupby(['OpenDate', 'Product Code', 'L-Lots', 'BidPrice', 'S-Lots', 'AskPrice', 'H/S'], as_index=False).first()
    # print(dsg)

    # dsg.to_csv("testing.csv", index=False, header=False)
    '''
    m_day = merge_df['trade date'].max()
    g_merge_df2 = merge_df.groupby(['price', 'Order Type', 'Commodity', 'carry leg', 'Trade type', 'direction'])['lots'].sum()
    # g_merge_df3 = merge_df.groupby(['price', 'Order Type', 'Commodity', 'carry leg', 'Trade type']).apply(lambda t: t[t['Unique trade ID']==t['Unique trade ID'].min()])
    # g_merge_df4 = merge_df.groupby(['price', 'Order Type', 'Commodity', 'carry leg', 'Trade type']).apply(lambda t: t['lots'])
    g_merge_df5 = merge_df.groupby(['price', 'Order Type', 'Commodity', 'carry leg', 'Trade type', 'direction'], as_index=False).first()
    ans2 = [x for x in g_merge_df2]
    '''
    # df.to_csv(outputPath, index=False)
    return
def test(myArr = []):
    myArr.append(1)
    print(myArr)
    return
def main():
    global strYMD
    # year = 2017
    # month = 6
    # day = 13
    dueConfigPath = "./config/date.txt"
    g_dueFunds = initialDueConfig(dueConfigPath)
    presentYear = g_dueFunds['year']
    presentMon = g_dueFunds['month']
    presentday = g_dueFunds['day']
    presentYMD = date(int(presentYear), int(presentMon), int(presentday))
    # "12/6/2017"
    # date_time.strftime('%Y-%m-%d')

    strYMD = presentYMD.strftime('%d/%m/%Y')
    print(strYMD)
    filter(date=strYMD)
    print("Hello world")
    return 0

if __name__=="__main__":
    main()

