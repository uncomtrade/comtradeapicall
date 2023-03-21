import requests
import time as t
import pandas
from pandas import json_normalize

def getAIS(subscription_key, typeCode='C', freqCode='D', clCode='XX', countryareaCode='', vesselTypeCode='', flowCode='', dateFrom='', dateTo='', maxRecords=250000):
    baseURL = 'https://comtradeapi.un.org/experimental/v1/getAIS/' + typeCode + '/' + freqCode + '/' + clCode
 
    PARAMS = dict(countryareaCode=countryareaCode, vesselTypeCode=vesselTypeCode,
                  flowCode=flowCode, dateFrom=dateFrom, dateTo=dateTo,
                  maxRecords=maxRecords)
    # add key
    PARAMS["subscription-key"] = subscription_key
    # print(PARAMS)
    # only for JSON format
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
