import pandas as pd
import json
from pandas import json_normalize
import urllib3



def getMetadata(subscription_key, typeCode, freqCode, clCode, period, reporterCode, showHistory):
    baseURL = 'https://comtradeapi.un.org/data/v1/getMetadata/' + typeCode + '/' + freqCode + '/' + clCode
    PARAMS = dict(reporterCode=reporterCode, period=period)
    PARAMS["subscription-key"] = subscription_key
    fields = dict(filter(lambda item: item[1] is not None, PARAMS.items()))  
    http = urllib3.PoolManager()
    try:
        resp = http.request("GET",baseURL, fields = fields, timeout=120)
        if resp.status != 200:
            print(resp.data.decode('utf-8'))
        else:
            jsonResult = json.loads(resp.data)
            df = json_normalize(jsonResult['data'])
            FIELDS = ['notes']
            dt = df[FIELDS]
            dt = dt.explode('notes')
            df_final = (
                pd.DataFrame(dt["notes"]
                             .apply(pd.Series))
            )
            dt_final_latest = df_final[['datasetCode', 'publicationDate']].groupby("datasetCode").max()
            dt_final_latest.loc[:, 'isLatestPublication'] = True
            df_final_merge = df_final.merge(dt_final_latest, on='publicationDate', how='left')
            if (showHistory == True):
                return df_final_merge
            else:
                return df_final_merge[df_final_merge.notnull()].query('isLatestPublication==True')
    except urllib3.exceptions.RequestError as err:
        print(f'Request error: {err}')

def listReference(category=None):
    baseURL = 'https://comtradeapi.un.org/files/v1/app/reference/ListofReferences.json'
    try:
        http = urllib3.PoolManager()
        resp = http.request("GET",baseURL, timeout=120)
        if resp.status != 200:
            print(resp.data.decode('utf-8'))
        else:
            resp.encoding = 'utf-8-sig'
            jsonResult = json.loads(resp.data)
            df = json_normalize(jsonResult['results'])
            if category is not None:
                return df.query("category=='" + category + "'")
            else:
                return df
    except urllib3.exceptions.RequestError as err:
        print(f'Request error: {err}')

def getReference(category):
    try:
        baseURL = listReference(category).iloc[0]['fileuri']
    except:
        baseURL = ''
        print('Error in looking up the file URI for', category)
    if baseURL != '':
        try:
            http = urllib3.PoolManager()
            resp = http.request("GET",baseURL, timeout=120)
            if resp.status != 200:
                print(resp.data.decode('utf-8'))
            else:
                resp.encoding = 'utf-8-sig'
                jsonResult = json.loads(resp.data)
                df = json_normalize(jsonResult['results'])  # Results contain the required data
                return df
        except urllib3.exceptions.RequestError as err:
            print(f'Request error: {err}')

def convertCountryIso3ToCode(countryIsoCode):
    baseURL = 'https://comtradeapi.un.org/files/v1/app/reference/country_area_code_iso.json'
    http = urllib3.PoolManager()
    resp = http.request("GET",baseURL, timeout=120)
    df = json_normalize(json.loads(resp.data)['results'])
    df['country_area_code'] = df['country_area_code'].astype(str)
    delim = ','
    iso_string = countryIsoCode
    iso_list = iso_string.split(delim)
    code_list = df[df['iso3'].isin(iso_list)]['country_area_code'].tolist()
    code_string = delim.join(code_list)
    return code_string
