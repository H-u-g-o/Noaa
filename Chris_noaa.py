import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import tarfile
import gzip


class getDf:
    '''Class spécifique Températures - Création DF brut'''
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


class dfExls:
    '''Class spécifique Consommation - Création DF brut'''
    def __init__(self, year, region):
        self.year = str(year)
        self.region = str(region)

    def getConso(self):
        conso_df = pd.read_table('RAW_DATA_RTE/RTE_' + self.region +'_'+ self.year + '.xls', encoding='ISO-8859-1', skiprows=1, header=None)
        return conso_df


class DataCleanT:
    '''Class spécifique Températures - Nettoyage Data'''
    def __init__(self,df):
        self.df = df

    def main(self):
        '''Lancement automatique de l'ensemble des méthodes'''
        self.col()
        self.no_data()
        self.clean()
        self.celsius()
        self.conver_date()
        #self.index_date()
        return self.df

    def col(self):
        '''Attribution noms des colonnes'''
        columns = ['STN', 'WBAN', 'YEARMODA', 'TEMP', 'COUNT_1', 'DEWP', 'COUNT_2', 'SLP', 'COUNT_3', 'STP', 'COUNT_4',
                   'VISIB', 'COUNT_5', 'WDSP', 'COUNT_6', 'MXSPD', 'GUST', 'MAX', 'MIN', 'PRCP', 'SNDP', 'FRSHTT']
        self.df.columns=columns

    def no_data(self):
        '''Remplacement des données manquantes par Nan'''
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

    def clean(self):
        '''Suppression des * dans les colonnes MIN et MAX'''
        self.df['MIN'] = self.df['MIN'].apply(lambda x: x.replace('*', ''))
        self.df['MAX'] = self.df['MAX'].apply(lambda x: x.replace('*', ''))

    def celsius(self):
        '''Conversion de la température en degré Celsius'''
        self.df['Temp_C'] = round((self.df['TEMP'] - 32) / 1.8, 2)
        return self.df

    def conver_date(self):
        '''Conversion de la colonne TEMP en format date'''
        self.df["YEARMODA"] = pd.to_datetime(self.df["YEARMODA"], format='%Y%m%d')
        self.df = self.df.rename(columns={"YEARMODA": "Date"})

    def index_date(self):
        '''Passe la col date en index'''
        self.df = self.df.set_index('YEARMODA')


class DataCleanC:
    '''Class spécifique Conso - Nettoyage Data'''
    def __init__(self,df):
        self.df = df

    def main(self):
        '''Lancement automatique de l'ensemble des méthodes'''
        self.col()
        self.drop_col()
        self.index_date()
        return self.df

    def col(self):
        '''Attribution noms des colonnes'''
        col = ['Périmètre', 'Nature', 'Date', 'Heures', 'Consommation', 'Thermique',
               'Nucléaire', 'Eolien', 'Solaire', 'Hydraulique', 'Pompage',
               'Bioénergies', 'Ech. physiques', 'exit']
        self.df.columns=col

    def drop_col(self):
        '''Suppression col inutiles'''
        self.df = self.df.drop(columns=['exit'])
        self.df = self.df.drop(columns=['Heures'])

    def index_date(self):
        '''Groupby date   +  index=date'''
        self.df = round(self.df.groupby('Date').mean(), 2)
        self.df = self.df.reset_index()
        self.df["Date"] = pd.to_datetime(self.df["Date"], format='%Y-%m-%d')


class Noaa:
    '''Class spécifique Températures - création DF clean'''
    def __init__(self, yearBegin, yearEnd, station):
        self.yearBegin = int(yearBegin)
        self.yearEnd = int(yearEnd)
        self.station = str(station)

    def getSeveralyear(self):
        '''Création d'un CSV concat et clean'''
        list_df = []
        for i in range (self.yearBegin, self.yearEnd+1):
            n = getDf(i, self.station)
            n.getYear()
            list_df.append(n.getStation())
        all_data_df = pd.concat(list_df, ignore_index=True)

        # appelle de la class DataClean
        all_data_df = DataCleanT(all_data_df)
        all_data_df = all_data_df.main()
        #all_data_df.to_csv(str(self.yearBegin)+'_'+str(self.yearEnd)+'_'+str(self.station)+'.csv')
        return all_data_df


class Rte:
    '''Class spécifique Températures _ création DF clean'''
    def __init__(self, yearBegin, yearEnd, region):
        self.yearBegin = int(yearBegin)
        self.yearEnd = int(yearEnd)
        self.region = str(region)

    def getSeveralyear(self):
        '''Création d'un CSV concat et clean'''
        list_df = []
        for i in range (self.yearBegin, self.yearEnd+1):
            n = dfExls(i, self.region)
            list_df.append(n.getConso())
        all_data_c = pd.concat(list_df, ignore_index=True)

        # appelle de la class DataClean
        all_data_c = DataCleanC(all_data_c)
        all_data_c = all_data_c.main()
        #all_data_c.to_csv(str(self.yearBegin)+'_'+str(self.yearEnd)+'_'+str(self.region)+'.csv')
        return all_data_c


class Visu:
    '''Class globale - Création graphiques à partir d'un CSV'''
    def __init__(self, csv):
        self.csv = csv

    def dataframe(self):
        '''Transforme le CSV en dataframe'''
        self.df = pd.read_csv(self.csv)
        return self.df

    def graphTemp(self):
        '''Graph de l'évolution de la température'''
        self.df = pd.read_csv(self.csv)
        plt.figure(figsize=(20, 12))
        sns.barplot(x="YEARMODA", y="Temp_C", data=self.df, color="indianred")
        plt.xlabel('\nDates', fontsize=14)
        plt.xticks(fontsize=2, rotation=90, horizontalalignment='right')
        plt.ylabel('Températures\n', fontsize=14)
        plt.title('Evolution des températures \n', fontsize=18, fontweight=600)
        plt.savefig('VISUprojet/' +'Temp_'+ self.csv + '.png')

    def graphConso(self):
        '''Graph de l'évolution de la consommation d'énergie'''
        self.df = pd.read_csv(self.csv)
        plt.figure(figsize=(20, 12))
        sns.barplot(x="Date", y="Consommation", data=self.df, color="lightblue")
        plt.xlabel('\nDates', fontsize=14)
        plt.xticks(fontsize=2, rotation=90, horizontalalignment='right')
        plt.ylabel('Consommation\n', fontsize=14)
        plt.title('Consommation énergie\n', fontsize=18, fontweight=600)
        plt.savefig('VISUprojet/' +'Conso_'+ self.csv + '.png')

    def graphGroupir(self):
        '''Graph de l'évolution température + conso énergie'''
        self.df = pd.read_csv(self.csv)
        fig = plt.figure(figsize=(20, 12))
        sns.lineplot(x="Date", y="Temp_C", data=self.df, color="indianred")
        plt.xlabel('\nTemps', fontsize=18)
        plt.xticks(fontsize=2, rotation=90, horizontalalignment='right')
        plt.ylabel('Températures\n', fontsize=18)
        ax2 = plt.twinx()
        sns.lineplot(x="Date", y="Consommation", data=self.df, ax=ax2,color="steelblue")
        plt.ylabel('\nConsommation', fontsize=18)
        fig.legend(labels=['Températures', 'Consommation'],fontsize=18)
        plt.title('Températures & Consommation\n', fontsize=24, fontweight=600)
        plt.savefig('VISUprojet/' + self.csv + '.png')


class DataFinal:
    '''Class globale - création CSV groupé final'''
    def __init__(self, yearBegin, yearEnd, df1, df2, region):
        self.yearBegin = yearBegin
        self.yearEnd = yearEnd
        self.df1 = df1
        self.df2 = df2
        self.region = region
        self.dfDef = None

    def mergeDf(self):
        dfDef = pd.merge(self.df1, self.df2, on='Date', how='inner')
        dfDef.to_csv(str(self.yearBegin) +'_'+ str(self.yearEnd) +'_' + self.region +'.csv')
        return dfDef


class MegaClass:
    "Class globale qui coordonne toutes les autres class"
    def __init__(self, yearBegin, yearEnd, station, region):
        self.yearBegin = int(yearBegin)
        self.yearEnd = int(yearEnd)
        self.station = str(station)
        self.region = str(region)

    def main(self):
        c = Rte(self.yearBegin, self.yearEnd, self.region)
        dfc = c.getSeveralyear()
        t = Noaa(self.yearBegin, self.yearEnd, self.station)
        dft = t.getSeveralyear()
        dff = DataFinal(self.yearBegin, self.yearEnd, dft, dfc, self.region)
        dff = dff.mergeDf()
        v = Visu(str(self.yearBegin) +'_'+ str(self.yearEnd) +'_' + self.region +'.csv')
        v.graphGroupir()


m = MegaClass(2014, 2017, "072220-99999", "Pays-de-la-Loire")
m.main()


# PARIS = 071560-99999                  // Ile-de-France
# AVIGNON = 075630-99999                // PACA
# AIX LES BAINS = 074910-99999          // ARA
# REIMS = 070720-99999                  // Grand-Est
# PAU = 076100-99999                    // Nouvelle-Aquitaine
# BESANCON = 072880-99999               // Bourgogne-Franche-Comté
# VANNES/SENE = 072100-99999            // Bretagne
# BOURGES = 072550-99999                // Centre-Val-de-Loire
# MONTAUBAN = 075400-99999              // Occitanie
# CALAIS/DUNKERQUE = 70090-99999        // Hauts-de-France
# LE HAVRE-OCTEVILLE = 070460-99999     // Normandie
# NANTES ATLANTIQUE = 072220-99999      // Pays-de-la-Loire
