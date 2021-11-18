#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import pandas as pd
from zipfile import ZipFile


from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

local_path = "..//Input//Conector 139//"


# In[2]:


def get_data(url, ZipFilename, CSVfilename):    
    r = requests.get(url)
    open(ZipFilename, "wb").write(r.content)
    with ZipFile(ZipFilename, 'r') as zipObj:
        zipObj.extract(CSVfilename, local_path)
    return True

url = 'https://www.bis.org/statistics/full_webstats_cbpol_d_dataflow_csv_row.zip'
ZipFilename = local_path + 'full_webstats_cbpol_d_dataflow_csv_row.zip'
CSVfilename = 'WEBSTATS_CBPOL_D_DATAFLOW_csv_row.csv'
get_data(url, ZipFilename, CSVfilename)

url = 'https://www.bis.org/statistics/full_bis_selected_pp_csv.zip'
ZipFilename = local_path + 'full_bis_selected_pp_csv.zip'
CSVfilename = 'WEBSTATS_SELECTED_PP_DATAFLOW_csv_col.csv'
get_data(url, ZipFilename, CSVfilename)

url = 'https://www.bis.org/statistics/full_webstats_eer_d_dataflow_csv_row.zip'
ZipFilename = local_path + 'full_webstats_eer_d_dataflow_csv_row.zip'
CSVfilename = 'WEBSTATS_EER_D_DATAFLOW_csv_row.csv'
get_data(url, ZipFilename, CSVfilename)


# In[10]:


url = 'https://www.bis.org/statistics/full_bis_total_credit_csv.zip'
ZipFilename = local_path + 'full_bis_dsr_csv.zip'
CSVfilename = 'WEBSTATS_TOTAL_CREDIT_DATAFLOW_csv_col.csv'
get_data(url, ZipFilename, CSVfilename)


# In[3]:


#139 Policy rates : 
#140 nominal Propery Prices : 
#141 Real Propery Prices 
#142 Effective exchange rate narrow basket 
#143 Effective exchatnge rate broad basket


# In[4]:


df = pd.read_csv(local_path + 'WEBSTATS_CBPOL_D_DATAFLOW_csv_row.csv')
df = df.drop(df.index[[1, 3]])
headers = df.iloc[0]
nombres = 'Policy rates - ' + headers.str[3:]
nombres[0] = 'Date'
df  = pd.DataFrame(df.values[1:], columns=nombres)
df = df.set_index(['Date'])
df["country"] = "none"

alphacast.datasets.dataset(139).upload_data_from_df(df, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)


# In[5]:


df = pd.read_csv(local_path + 'WEBSTATS_SELECTED_PP_DATAFLOW_csv_col.csv')
df = df.T
df.drop(index=['FREQ', 'Frequency', 'REF_AREA', 'VALUE', 'UNIT_MEASURE', 'Time Period'], inplace=True)
headers = df.iloc[0]
headers2= df.iloc[1]
headers3 = df.iloc[2]
nombres = 'Property Prices - ' + headers + " - " + headers2 + " - " + headers3
nombres[0] = 'Date'
df  = pd.DataFrame(df.values[3:], columns=nombres)
df["Date"] = pd.date_range(start='1/1/1927', periods = len(df), freq='Q')
df = df.set_index(['Date'])
df_nominal = df.filter(regex='Nominal')
df_real = df.filter(regex='Real')
df_real["country"] = "none"
df_nominal["country"] = "none"

alphacast.datasets.dataset(140).upload_data_from_df(df_nominal, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)
alphacast.datasets.dataset(141).upload_data_from_df(df_real, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)


# In[6]:


df = pd.read_csv(local_path + 'WEBSTATS_EER_D_DATAFLOW_csv_row.csv')
df = df.drop(df.index[[0, 3]])
headers2 = df.iloc[0]
headers = df.iloc[1]
nombres = 'Effective Exchange Rate (by basket) - ' + headers.str[3:] + " - " + headers2.str[2:]
nombres[0] = 'Date'
df  = pd.DataFrame(df.values[2:], columns=nombres)
df = df.set_index(['Date'])
df_broad = df.filter(regex='Broad')
df_narrow = df.filter(regex='Narrow')
df_narrow
df_broad["country"] = "none"
df_narrow["country"] = "none"

alphacast.datasets.dataset(142).upload_data_from_df(df_broad, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)

alphacast.datasets.dataset(143).upload_data_from_df(df_narrow, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)

# In[78]:


df = pd.read_csv(local_path + 'WEBSTATS_TOTAL_CREDIT_DATAFLOW_csv_col.csv')
df = df.drop(df.index[[0, 3]])
headers2 = df.iloc[0]
headers = df.iloc[1]

df = df[df["Valuation"] == "Market value"]
df = df[df["Lending sector"] == "All sectors"]
df = df[df["Type of adjustment"] == "Adjusted for breaks"]

df.drop(columns=['FREQ', 'Frequency', 'BORROWERS_CTY', 'TC_BORROWERS', 'TC_LENDERS', 'Lending sector', 
                 "VALUATION", "Valuation", "TC_ADJUST", "Type of adjustment", "UNIT_TYPE", "Time Period"], inplace=True)


# In[79]:


df = df.melt(id_vars =["Borrowers' country", "Borrowing sector", "Unit type"])

df["Day"] = 1
df["Month"] = (df["variable"].str.split(pat="-Q", expand=True)[1].astype("int")  -1)*3 + 1
df["Year"] = df["variable"].str.split(pat="-Q", expand=True)[0]
df["Date"] = pd.to_datetime(df[["Year", "Month", "Day"]])
df["Unit - Sector"] = df["Unit type"] + " - " + df["Borrowing sector"]
df.drop(columns=['variable', 'Day', 'Month', 'Year', "Borrowing sector", "Unit type"], inplace=True)

df = df.set_index(["Date", "Borrowers' country", "Unit - Sector"]).unstack(["Unit - Sector"])
df.columns = df.columns.map(' - '.join)
for col in df.columns:
    df = df.rename(columns={col: col.replace("value - ", "")})
df = df.reset_index().rename(columns={"Borrowers' country": "Country / Region"})


# In[ ]:


# In[ ]:


#dataset_name = "Financial - Global - BIS - Credit to the non-financial sector"
#description = "The BIS quarterly statistics on credit to the non-financial sector capture borrowing activity of the private non-financial sector (published since March 2013) and the government sector (published since September 2015) in more than 40 economies. Data on credit to the government sector cover, on average, 20 years, and those on credit to the private non-financial sector cover, on average, more than 45 years."
#dataset = alphacast.datasets.create(dataset_name, 22, description)
#alphacast.datasets.dataset(8134).initialize_columns(dateColumnName = "Date", entitiesColumnNames=["Country / Region"], dateFormat= "%Y-%m-%d")        


# In[86]:


alphacast.datasets.dataset(8134).upload_data_from_df(df, deleteMissingFromDB = False, onConflictUpdateDB = True, uploadIndex=False)


# In[119]:


df = pd.read_csv(local_path + 'WEBSTATS_TOTAL_CREDIT_DATAFLOW_csv_col.csv')
df = df.drop(df.index[[0, 3]])
headers2 = df.iloc[0]
headers = df.iloc[1]


# In[120]:


df = df[df["Borrowing sector"] == "General government"]
df = df[df["Type of adjustment"] == "Adjusted for breaks"]
df["Unit - Valuation"] = df["Unit type"] + " - " + df["Valuation"]

df.drop(columns=['FREQ', 'Frequency', 'BORROWERS_CTY', 'TC_BORROWERS', 'TC_LENDERS', 'Lending sector', "Borrowing sector",
                 "VALUATION", "TC_ADJUST", "Type of adjustment", "UNIT_TYPE", "Time Period", "Valuation", "Unit type"], inplace=True)


# In[121]:


df = df.melt(id_vars =["Borrowers' country", "Unit - Valuation"])

df["Day"] = 1
df["Month"] = (df["variable"].str.split(pat="-Q", expand=True)[1].astype("int")  -1)*3 + 1
df["Year"] = df["variable"].str.split(pat="-Q", expand=True)[0]
df["Date"] = pd.to_datetime(df[["Year", "Month", "Day"]])
df.drop(columns=['variable', 'Day', 'Month', 'Year'], inplace=True)


# In[122]:


df = df.set_index(["Date", "Borrowers' country", "Unit - Valuation"]).unstack(["Unit - Valuation"])
df.columns = df.columns.map(' - '.join)
for col in df.columns:
    df = df.rename(columns={col: col.replace("value - ", "")})
df = df.reset_index().rename(columns={"Borrowers' country": "Country / Region"})


# In[123]:


#dataset_name = "Financial - Global - BIS - Credit to the general Government - Nominal & Market value"
#description = "The BIS quarterly statistics on credit to the non-financial sector capture borrowing activity of the private non-financial sector (published since March 2013) and the government sector (published since September 2015) in more than 40 economies. Data on credit to the government sector cover, on average, 20 years, and those on credit to the private non-financial sector cover, on average, more than 45 years."
#dataset = alphacast.datasets.create(dataset_name, 22, description)
#alphacast.datasets.dataset(8135).initialize_columns(dateColumnName = "Date", entitiesColumnNames=["Country / Region"], dateFormat= "%Y-%m-%d")        


# In[126]:


alphacast.datasets.dataset(8135).upload_data_from_df(df, deleteMissingFromDB = False, onConflictUpdateDB = True, uploadIndex=False)


# In[ ]:




