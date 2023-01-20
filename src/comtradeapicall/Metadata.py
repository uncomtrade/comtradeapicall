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
            if (showHistory==True):
                return df_final_merge
            else:
                return  df_final_merge[df_final_merge.notnull()].query('isLatestPublication==True')
    except requests.exceptions.Timeout:
        # Maybe set up for a retry, or continue in a retry loop
        print('Request failed due to timeout')
    except requests.exceptions.TooManyRedirects:
        # Tell the user their URL was bad and try a different one
        print('Request failed due to too many redirects')
    except requests.exceptions.RequestException as e:
        # catastrophic error. bail.
        raise SystemExit(e)
