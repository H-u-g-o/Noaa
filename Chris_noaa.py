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
        with tarfile.open('RAW_DATA/noaa/gsod_all_years/gsod_'+self.year+'.tar') as tar:
            tar.extractall(path='./temp_'+self.year)

    def getStation(self):
        with gzip.open('./temp_'+self.year+'/'+self.station+'-'+self.year+'.op.gz','rb') as station_file:
            for line in station_file:
                station_df = pd.read_csv(station_file, delim_whitespace = True, header = None)
                return station_df


class DataClean:

    def __init__(self,df):
        self.df = df

    def no_data(self):
        "Remplacement des données manquantes par Nan"
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
        return self.df

    def celsius(self):
        "Conversion de la température en degré Celsius"
        self.df['TEMP'] = self.df['TEMP'].replace("*", "")
        self.df['Temp_C'] = (self.df['TEMP'] - 32) / 1.8
        return self.df

    def conver_date(self):
        "Conversion de la colonne TEMP en format date"
        pass



class Noaa:

    def __init__(self, yearBegin, yearEnd, station):
        self.yearBegin = int(yearBegin)
        self.yearEnd = int(yearEnd)
        self.station = str(station)

    def getSeveralyear(self):
        list_df = []
        for i in range (self.yearBegin, self.yearEnd+1):
            n = getDf(i, self.station)
            n.getYear()
            list_df.append(n.getStation())
        all_data_df = pd.concat(list_df, ignore_index=True)
        columns = ['STN', 'WBAN', 'YEARMODA', 'TEMP', 'COUNT_1', 'DEWP', 'COUNT_2', 'SLP', 'COUNT_3', 'STP', 'COUNT_4',
                   'VISIB', 'COUNT_5', 'WDSP', 'COUNT_6', 'MXSPD', 'GUST', 'MAX', 'MIN', 'PRCP', 'SNDP', 'FRSHTT']
        all_data_df.columns=columns
        # appelle de la class DataClean
        all_data_df = DataClean(all_data_df)
        df = all_data_df.no_data()
        df= all_data_df.celsius()

        print(df.info())

        df.to_csv(str(self.yearBegin)+'_'+str(self.yearEnd)+'.csv')


class visua:

    def __init__(self, csv):
        self.csv = csv

    def dataframe(self):
        """Transforme le csv en dataframe"""
        self.df = pd.read_csv(self.csv)
        return self.df

    def graphTemp(self):
        """Graph de l'évolution de la température sur 1 an"""
        self.df = pd.read_csv(self.csv)
        plt.figure(figsize=(20, 12))
        sns.barplot(x="YEARMODA", y="Temp_C", data=self.df, color="indianred")
        plt.xlabel('\nDates', fontsize=14)
        plt.xticks(fontsize=2, rotation=90, horizontalalignment='right')
        plt.ylabel('Températures\n', fontsize=14)
        plt.title('Températures & précipitations\n', fontsize=18, fontweight=600)
        plt.savefig('VISUprojet/' + self.csv + '.png')



essai = Noaa(2015,2018,"071560-99999")
essai.getSeveralyear()

test = visua("2015_2018.csv")
df1 = test.dataframe()
test.graphTemp()


