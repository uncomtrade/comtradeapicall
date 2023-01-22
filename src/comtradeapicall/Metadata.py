import json
import time as t
from datetime import datetime
import pandas as pd
from pandas import json_normalize
import requests
import urllib3


def getMetadata(subscription_key, typeCode, freqCode, clCode, period, reporterCode, showHistory):
    baseURL = 'https://comtradeapi.un.org/data/v1/getMetadata/' + typeCode + '/' + freqCode + '/' + clCode
    PARAMS = dict(reporterCode=reporterCode, period=period)
    # add key
    PARAMS["subscription-key"] = subscription_key
    # print(PARAMS)
    try:
        resp = requests.get(baseURL, params=PARAMS, timeout=120)
        # print(resp.text)
        # print(resp.url)
        if resp.status_code != 200:
            # This means something went wrong.
            jsonResult = resp.json()
            print('Error in calling API:', resp.url)
            try:
                print('Error code:', jsonResult['statusCode'])
                print('Error message:', jsonResult['message'])
            except:
                t.sleep(1)
        else:
            jsonResult = resp.json()
            df = json_normalize(jsonResult['data'])  # Results contain the required data
            # Get the notes only
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
    except requests.exceptions.Timeout:
        # Maybe set up for a retry, or continue in a retry loop
        print('Request failed due to timeout')
    except requests.exceptions.TooManyRedirects:
        # Tell the user their URL was bad and try a different one
        print('Request failed due to too many redirects')
    except requests.exceptions.RequestException as e:
        # catastrophic error. bail.
        raise SystemExit(e)

def listReference(category=None):
    baseURL = 'https://comtradeapi.un.org/files/v1/app/reference/ListofReferences.json'
    try:
        resp = requests.get(baseURL, timeout=120)
        # print(resp.text)
        # print(resp.url)
        if resp.status_code != 200:
            # This means something went wrong.
            try:
                print('Error in calling API:', resp.url)
            except:
                t.sleep(1)
        else:
            resp.encoding = 'utf-8-sig'
            jsonResult = resp.json()
            df = json_normalize(jsonResult['results'])  # Results contain the required data
            if category is not None:
                return df.query("category=='" + category + "'")
            else:
                return df
    except requests.exceptions.Timeout:
        # Maybe set up for a retry, or continue in a retry loop
        print('Request failed due to timeout')
    except requests.exceptions.TooManyRedirects:
        # Tell the user their URL was bad and try a different one
        print('Request failed due to too many redirects')
    except requests.exceptions.RequestException as e:
        # catastrophic error. bail.
        raise SystemExit(e)

def getReference(category):
    try:
        baseURL = listReference(category).iloc[0]['fileuri']
    except:
        baseURL = ''
        print('Error in looking up the file URI for', category)
    if baseURL != '':
        try:
            resp = requests.get(baseURL, timeout=120)
            # print(resp.text)
            # print(resp.url)
            if resp.status_code != 200:
                # This means something went wrong.
                try:
                    print('Error in calling API:', resp.url)
                except:
                    t.sleep(1)
            else:
                resp.encoding = 'utf-8-sig'
                jsonResult = resp.json()
                df = json_normalize(jsonResult['results'])  # Results contain the required data
                return df
        except requests.exceptions.Timeout:
            # Maybe set up for a retry, or continue in a retry loop
            print('Request failed due to timeout')
        except requests.exceptions.TooManyRedirects:
            # Tell the user their URL was bad and try a different one
            print('Request failed due to too many redirects')
        except requests.exceptions.RequestException as e:
            # catastrophic error. bail.
            raise SystemExit(e)

def convertCountryIso3ToCode(countryIsoCode):
    baseURL = 'https://comtradeapi.un.org/files/v1/app/reference/country_area_code_iso.json'
    resp = requests.get(baseURL, timeout=120)
    df = json_normalize(resp.json()['results'])
    df['country_area_code'] = df['country_area_code'].astype(str)
    delim = ','
    iso_string = countryIsoCode
    iso_list = iso_string.split(delim)
    code_list = df[df['iso3'].isin(iso_list)]['country_area_code'].tolist()
    code_string = delim.join(code_list)
    return code_string
