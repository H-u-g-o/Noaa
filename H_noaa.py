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

    def getConso(self):
        conso_df = pd.read_table('RTE/eCO2mix_RTE_Ile-de-France_Annuel-Definitif_'+self.year+ '.xls', encoding='ISO-8859-1', skiprows=1, header=None)
        return conso_df

class Noaa:

    def __init__(self, yearBegin=2016, yearEnd=2017, station="071560-99999"):
        self.yearBegin = int(yearBegin)
        self.yearEnd = int(yearEnd)
        self.station = str(station)

    def getSeveralyear(self):
        liste_df = []
        liste_df_conso = []
        for i in range (self.yearBegin, self.yearEnd+1):
            n = getDf(i, self.station )
            n.getYear()
            liste_df.append(n.getStation())
            n.getConso()
            liste_df_conso.append(n.getConso())
        all_data_df = pd.concat(liste_df, ignore_index=True)
        all_data_c = pd.concat(liste_df_conso, ignore_index=True)
        all_data_df = DataClean(all_data_df)
        all_data_df = all_data_df.main()
        return print(all_data_df.head())


class DataClean:

    def __init__(self, df):
        self.df = df

    def main(self):
        self.nameCol()
        self.no_data()
        self.celsius()
        self.removeSymbol()
        self.conver_date()
        self.index_date()
        return self.df

    def nameCol(self):
        columns = ['STN','WBAN','YEARMODA','TEMP','COUNT_1','DEWP','COUNT_2','SLP','COUNT_3','STP','COUNT_4','VISIB','COUNT_5','WDSP','COUNT_6','MXSPD','GUST','MAX','MIN','PRCP','SNDP','FRSHTT']
        self.df.columns=columns

    def no_data(self):
        """Remplacement des données manquantes par Nan"""
        self.df['TEMP'] = self.df['TEMP'].replace(9999.9, np.nan)
        self.df['DEWP'] = self.df['DEWP'].replace(9999.9, np.nan)
        self.df['SLP'] = self.df['SLP'].replace(9999.9, np.nan)
        self.df['STP'] = self.df['STP'].replace(9999.9, np.nan)
        self.df['VISIB'] = self.df['VISIB'].replace(999.9, np.nan)
        self.df['WDSP'] = self.df['WDSP'].replace(999.9, np.nan)
        self.df['MXSPD'] = self.df['MXSPD'].replace(999.9, np.nan)
        self.df['GUST'] = self.df['GUST'].replace(999.9, np.nan)
        self.df['MAX'] = self.df['MAX'].replace(9999.9, np.nan)
        self.df['MIN'] = self.df['MIN'].replace(9999.9, np.nan)
        self.df['PRCP'] = self.df['PRCP'].replace(99.9, np.nan)
        self.df['SNDP'] = self.df['SNDP'].replace(999.9, np.nan)

    def removeSymbol(self):
        """Remove unwanted symbols"""
        self.df['MAX'] = self.df['MAX'].apply(lambda x: x.replace('*',''))
        self.df['MIN'] = self.df['MIN'].apply(lambda x: x.replace('*',''))

    def celsius(self):
        """Conversion de la température en degré Celsius"""
        self.df['Temp_C'] = round((self.df['TEMP'] - 32) / 1.8)

    def conver_date(self):
        '''Conversion de la colonne TEMP en format date'''
        self.df["YEARMODA"] = pd.to_datetime(self.df["YEARMODA"], format='%Y%m%d')
        self.df = self.df.rename(columns={"YEARMODA": "Date"})

    def index_date(self):
        '''Passe la col date en index'''
        self.df = self.df.set_index('Date')


n = Noaa()
n.getSeveralyear()