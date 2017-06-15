# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import os
import time
import threading
from multiprocessing.pool import Pool

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

def filter(path = "./csv"):
    excelFileList = readAll(path, 'csv')
    # for afile in excelFileList:
    #     changeDate(afile)
    with Pool(8) as p:
        p.map(changeDate, excelFileList)
    return
def replaceDate(row):
    row["OpenDate"] = "test"
    return
def changeDate(path):
    print(path)
    dirsplit = os.path.split(path)
    name = dirsplit[-1]
    outputPath = "./output/" + name
    df = pd.read_csv(path, header=1)
    print(df["OpenDate"])
    print(df.head())
    # print(df.head)
    # df.apply(lambda x: np.nan if pd.isnull(x.Upper) \
    #     else 'U' if x.Price > x.Upper
    # else 'D' if x.Price < x.Lower \
    #     else 'M', axis=1)
    df["OpenDate"] =df["OpenDate"].apply(lambda x: "12/6/2017")
    print(df["OpenDate"])
    # df.loc[:, "OpenDate"] =
    # apply(lambda x: x.成交编号[4:] if x.交易所 == '郑商所' else x.成交编号, axis=1)
    # ds = df.apply(lambda x: x, print(x) )
    # print(ds)
    tar_date = '12/6/2017'
    arr = df.values
    df = df.fillna("N/A")
    # ds = df.iloc[[1,17],[0,2]]
    ds = df.ix[:, 0:17]
    ds.to_csv(outputPath, index=False)
    # df.to_csv(outputPath, index=False)
    return
def test(myArr = []):
    myArr.append(1)
    print(myArr)
    return
def main():

    filter()
    print("Hello world")
    return 0

if __name__=="__main__":
    main()

