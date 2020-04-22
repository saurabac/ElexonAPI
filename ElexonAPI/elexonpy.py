# -*- coding: utf-8 -*-
"""
Created on Wed Aug 20 12:06:23 2019
Elexon Data API
@author: Saurab Chhachhi
"""
######################### Libraries ###########################
from datetime import date, timedelta, datetime
import requests
import os
from io import StringIO
import pandas as pd
import fnmatch

######################### Functions ###########################
class API:
    def __init__(self, APIKEY):
        self.APIKEY = APIKEY

    def gen_url(self, report,start_date = '',end_date = '',\
    	period = '*', rtype = 0):
        server = 'https://api.bmreports.com/BMRS/'
        service = '&ServiceType=csv'
        api = '/V1?APIKey=' + self.APIKEY
        if rtype == 0:
            fdate = "&FromDate="
            tdate = "&ToDate="
            try: 
                start_date = start_date.strftime('%Y-%m-%d')
                end_date = end_date.strftime('%Y-%m-%d')
                url = server+report+api+fdate+start_date+tdate+end_date+service
            except:
                print('Date range not provided. Default data for yesterday and today ({} to {})'\
                      .format(date.today() - timedelta(days=1),date.today()))
                url = server+report+api+service
        elif rtype == 1:
            sdate = '&SettlementDate='
            speriod = '&Period='
            try: 
    	        start_date = start_date.strftime('%Y-%m-%d')
    	        url = server+report+api+sdate+start_date+speriod+period+service
            except:
                print('Date range not provided. Default data for latest period({} p {})'\
                      .format(date.today(), datetime.now().hour*2\
                      + int(datetime.now().minute/30)-1))
                url = server+report+api+service
        elif rtype == 2:
            fdate = "&FromSettlementDate="
            tdate = "&ToSettlementDate="
            speriod = '&Period='
            try: 
                start_date = start_date.strftime('%Y-%m-%d')
                end_date = end_date.strftime('%Y-%m-%d')
                url = server+report+api+fdate+start_date+tdate\
                		+end_date+speriod+period+service
            except:
                print('Date range not provided. Default data for yesterday and today ({} to {})'\
                      .format(date.today() - timedelta(days=1),date.today()))
                url = server+report+api+service
        elif rtype == 3:
            year = "&Year=" + str(start_date)
            try: 
                url = server+report+api+year+service
            except:
                print('Year not provided. Default data current year ({})'\
                      .format(date.today().year))
                url = server+report+api+service
        elif rtype == 4:
            sdate = '&SettlementDate='
            speriod = '&SettlementPeriod='
            try: 
                start_date = start_date.strftime('%Y-%m-%d')
                url = server+report+api+sdate+start_date+speriod+period+service
            except:
                print('Date range not provided. Default data for latest period({} p {})'\
                      .format(date.today(), datetime.now().hour*2\
                      + int(datetime.now().minute/30)-1))
                url = server+report+api+service
        return url
        
    def get_generation__by_fuel(self, start_date = '',end_date =''):
        # BMRS Half Hourly Outturn Generation by Fuel Type
        # 5.2.17 of API Guide pg. 84
        rtype = 0
        report = 'FUELHH'
        names = ['Record Type', 'Settlement Date', 'Settlement Period',\
        		'CCGT', 'OIL', 'COAL', 'NUCLEAR', 'WIND', 'PS', 'NPSHYD',\
        		'OCGT', 'OTHER', 'INTFR', 'INTIRL', 'INTNED', 'INTEW',\
        		'BIOMASS', 'INTNEM']
        #Generate URL
        url = self.gen_url(report,start_date = start_date,\
        				end_date = end_date, rtype = rtype)
        r = requests.get(url)
        data = pd.read_csv(StringIO(r.text), header=None,\
        					names = names, skiprows=1)
        solar = self.get_solar(start_date = start_date, end_date = end_date)
        # Format data
        data = data.iloc[:-1]
        data['Time'] = data['Settlement Period'].apply(lambda x:\
        								pd.Timedelta(str((x-1)*30)+' min'))
        data.index = pd.to_datetime(data['Settlement Date'],\
        								format = '%Y%m%d') + data['Time']
        data.drop(['Record Type', 'Time'], axis = 1, inplace = True)
        data['SOLAR'] = solar
        return data

    def get_solar(self, start_date = '', end_date = ''):
        sdate = start_date.strftime('%Y-%m-%dT%H:%M:%S')
        edate = end_date.strftime('%Y-%m-%dT%H:%M:%S')
        names = ['PES ID', 'DATETIME', 'SOLAR']
        url = 'https://api0.solar.sheffield.ac.uk/pvlive/v2?start='\
                + sdate +'&end=' + edate + '&data_format=csv'
        r = requests.get(url)
        data = pd.read_csv(StringIO(r.text), names = names, skiprows = 1)
        data.index = pd.to_datetime(data['DATETIME'])
        data.drop(['PES ID', 'DATETIME'], axis = 1, inplace = True)
        data['SOLAR'] = data['SOLAR']
        return data

    def get_actual_demand(self, start_date = '',period ='*'):
        # BMRS Actual Total Load per Bidding Zone
        # 5.1.12 of API Guide pg. 24
        rtype = 1
        report = 'B0610'
        names = ['TimeSeriesID','Settlement Date','Settlement Period',\
        		'Quantity','Secondary Quantity(MAW)','Document Type',\
                'Business Type', 'Process Type', 'Object Aggregation',\
                'Curve Type','Resolution','Unit Of Measure',\
                'Document ID','Document RevNum','ActiveFlag']
        #Generate URL
        url = self.gen_url(report,start_date = start_date,\
        				period = period, rtype = rtype)
        r = requests.get(url)
        data = pd.read_csv(StringIO(r.text), header=None,\
        				names = names, skiprows=1)
        # Format data
        data = data.iloc[4:-1]
        data = data[['Settlement Date','Settlement Period','Quantity']]
        data['Quantity'] = pd.to_numeric(data['Quantity'])
        data['Settlement Period'] = pd.to_numeric(data['Settlement Period'])
        data = data.sort_values('Settlement Period')
        data['Time'] = data['Settlement Period'].apply(lambda x:\
        								pd.Timedelta(str((x-1)*30)+' min'))
        data.index = pd.to_datetime(data['Settlement Date'],\
        					format = '%Y-%m-%d') + data['Time']
        data.drop('Time',axis = 1, inplace = True)
        data.rename({'Quantity': 'Actual'}, axis = 'columns', inplace = True)
        return data

    def get_dayahead_demand(self, start_date = '',period ='*'):
        # BMRS Day-Ahead Total Load Forecast per Bidding Zone
        # 5.1.13 of API Guide pg. 25
        rtype = 1
        report = 'B0620'
        names = ['TimeSeriesID','Settlement Date','Settlement Period',\
        		'Quantity','Document Type', 'Business Type',\
        		'Process Type', 'Object Aggregation','Resolution',\
        		'Curve Type','Unit Of Measure', 'ActiveFlag',\
        		'Document ID','Document RevNum','Secondary Quantity(MAW)']
        #Generate URL
        url = self.gen_url(report,start_date = start_date,\
        						period = period, rtype = rtype)
        r = requests.get(url)
        data = pd.read_csv(StringIO(r.text), header=None,\
        						names = names, skiprows=1)
        # Format data
        data = data.iloc[4:-1]
        data = data[['Settlement Date','Settlement Period','Quantity']]
        data['Settlement Period'] =\
        						pd.to_numeric(data['Settlement Period'])
        data = data.sort_values('Settlement Period')
        data['Quantity'] = pd.to_numeric(data['Quantity'])
        data['Time'] = data['Settlement Period'].apply(lambda x:\
        							pd.Timedelta(str((x-1)*30)+' min'))
        data.index = pd.to_datetime(data['Settlement Date'],\
        							format = '%Y-%m-%d') + data['Time']
        data.drop('Time',axis = 1, inplace = True)
        data.rename({'Quantity': 'Forecast'},\
        							axis = 'columns', inplace = True)
        return data

    def get_system_prices(self, start_date = '',end_date =''):
        # BMRS Derived System Wide Data
        # 5.2.51 of API Guide pg. 169
        rtype = 2
        report = 'DERSYSDATA'
        names = ['Record Type', 'Settlement Date', 'Settlement Period',\
        		'SSP', 'SBP', 'BD', 'PDC', 'RSP', 'NIV', 'SPA', 'BPA',\
        		'RP', 'RPRV', 'OV', 'BV', 'TOV', 'TBV','ASV','ABV',\
        		'TASV','TABV']
        #Generate URL
        url = self.gen_url(report, start_date = start_date,\
        				end_date = end_date, period = '*', rtype = rtype)
        r = requests.get(url)
        data = pd.read_csv(StringIO(r.text), header=None, names = names)
        # Format data
        data = data.iloc[1:-1]
        data['Time'] = data['Settlement Period'].apply(lambda x:\
        						pd.Timedelta(str((x-1)*30)+' min'))
        data.index = pd.to_datetime(data['Settlement Date'],\
        						format = '%Y%m%d') + data['Time']
        data.drop(['Record Type', 'Time'], axis = 1, inplace = True)
        return data

    def get_bo_stack(self, start_date = '',period =''):
        # BMRS Detailed System Price Data
        # 5.2.52 of API Guide pg. 177
        rtype = 4
        report = 'DETSYSPRICES'
        names = ['Record Type', 'Settlement Date', 'Settlement Period',\
        		'INDEX', 'ID','Acc ID', 'BOP ID', 'CADL Flag', 'SO Flag',\
        		'STOR Flag', 'Reprice', 'RSP','BO Price', 'BO Volume',\
        		'DMAT Vol', 'Arb Vol', 'NIV Vol', 'PAR Vol',\
        		'Final Price', 'TLM', 'TLM Adj Vol', 'TLM Adj Price']
        #Generate URL
        url = self.gen_url(report,start_date = start_date,\
        							period = str(period), rtype = rtype)
        try:
            r = requests.get(url)
            data = pd.read_csv(StringIO(r.text), header=None, names = names)
            #Format data
            data = data.iloc[1:-1]
            data['Time'] = data['Settlement Period'].apply(lambda x:\
            							pd.Timedelta(str((x-1)*30)+' min'))
            data.index = pd.to_datetime(data['Settlement Date'],\
            							format = '%Y%m%d') + data['Time']
            data = data[['Settlement Date', 'Settlement Period','BO Price',\
                        'BO Volume', 'Arb Vol', 'NIV Vol','Final Price']]
        except:
            names = ['Settlement Date', 'Settlement Period','BO Price',\
                        'BO Volume', 'Arb Vol', 'NIV Vol','Final Price']
            data = pd.DataFrame(data = [], columns = names)
        return data

    def get_market_prices(self, start_date = '', end_date = '',\
    	period ='*'):
        # BMRS Market Index Data
        # 5.2.8 of API Guide pg. 69
        rtype = 2
        report = 'MID'
        names = ['Record Type', 'Data Provider', 'Settlement Date',\
        		'Settlement Period','Price', 'Volume']
        #Generate URL
        url = self.gen_url(report,start_date = start_date,\
        		end_date=end_date, period = str(period), rtype = rtype)
        r = requests.get(url)
        data = pd.read_csv(StringIO(r.text), header=None, names = names)
        #Format data
        data = data.iloc[1:-1]
        data['Time'] = data['Settlement Period'].apply(lambda x:\
        							pd.Timedelta(str((x-1)*30)+' min'))
        data.index = pd.to_datetime(data['Settlement Date'],\
        							format = '%Y%m%d') + data['Time']
        data = data[data['Data Provider'] == 'APXMIDP'] # Only keep APXMIDP
        data.drop(['Record Type','Data Provider', 'Time'],\
        							axis = 1, inplace = True)
        return data

    def get_temperature(self,start_date = '',end_date =''):
        # BMRS Derived System Wide Data
        # 5.2.51 of API Guide pg. 169
        rtype = 0
        report = 'TEMP'
        names = ['Record Type', 'Settlement Date', 'Temp', 'Temp_Norm',\
        		'Temp_Low', 'Temp_High']
        #Generate URL
        url = self.gen_url(report,start_date = start_date,\
        				end_date = end_date, rtype = rtype)
        r = requests.get(url)
        data = pd.read_csv(StringIO(r.text), header=None, names = names)
        # Format data
        data = data.iloc[1:-1].copy(deep = True)
        data.index = pd.to_datetime(data['Settlement Date'],\
        				format = '%Y%m%d')
        data.drop(['Record Type'], axis = 1, inplace = True)
        return data

    def get_installed_cap(self, year = ''):
    	# BMRS Installed Generation Capacity Aggregated
        # 5.1.18 of API Guide pg. 31
        rtype = 3
        report = 'B1410'
        names = ['Document Type', 'Business Type', 'Process Type', 'TimeSeriesID',\
        		'Quantity', 'Resolution', 'Year', 'Power System Resource Type',\
        		'ActiveFlag','DocumentID', 'Document RevNum']
        #Generate URL
        url = self.gen_url(report,start_date = year, rtype = rtype)
        r = requests.get(url)
        data = pd.read_csv(StringIO(r.text), header=None, names = names)
        # Format data
        data = data[['Year','Power System Resource Type','Quantity',]].iloc[5:-1].copy(deep = True)
        data['Quantity'] = pd.to_numeric(data['Quantity']) 
        data = data.pivot(index = 'Year', columns = 'Power System Resource Type', values = 'Quantity')
        return data
    	