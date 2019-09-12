import numpy as np
import pandas as pd
import tarfile
import gzip
import re
import os
import datetime as dt
import matplotlib.pyplot as plt
import seaborn as sns
import tarfile
import gzip

class getDf:

    def __init__(self, year, station):
        self.year = str(year)
        self.station = str(station)

    def getYear(self):
        with tarfile.open('./gsod_all_years/gsod_'+self.year+'.tar') as tar:
            tar.extractall(path='./temp_'+self.year)

    def getStation(self):
        with gzip.open('./temp_'+self.year+'/'+self.station+'-'+self.year+'.op.gz','rb') as station_file:
            for line in station_file:
                station_df = pd.read_csv(station_file, delim_whitespace = True, header=None)
        return station_df


class Noaa:

    def __init__(self, yearBegin=2018, yearEnd=2019, station="071560-99999"):
        self.yearBegin = int(yearBegin)
        self.yearEnd = int(yearEnd)
        self.station = str(station)

    def getSeveralyear(self):
        liste_df = []
        for i in range (self.yearBegin, self.yearEnd+1):
            n = getDf(i, self.station )
            n.getYear()
            liste_df.append(n.getStation())
            print(liste_df)
        all_data = pd.concat(liste_df, ignore_index=True)
        columns = ['STN','WBAN','YEARMODA','TEMP','COUNT_1','DEWP','COUNT_2','SLP','COUNT_3','STP','COUNT_4','VISIB','COUNT_5','WDSP','COUNT_6','MXSPD','GUST','MAX','MIN','PRCP','SNDP','FRSHTT']
        all_data.columns=columns
        return all_data