import json
import pandas
from pandas import json_normalize
import requests
import time as t

def getLiveUpdate(subscription_key):
    baseURL = 'https://comtradeapi.un.org/data/v1/getLiveUpdate'
    # add key
    PARAMS = dict()
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
            # print(df.head())
            # print(df.describe())
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

def getDataAvailability(subscription_key, tradeDataType, dataAvailabilityType, typeCode, freqCode, clCode, period, reporterCode, publishedDateFrom, publishedDateTo):
    if dataAvailabilityType=='BULK':
        if tradeDataType == 'TARIFFLINE':
            baseURL = 'https://comtradeapi.un.org/bulk/v1/getTariffline/' + typeCode + '/' + freqCode + '/' + clCode
        else:
            baseURL = 'https://comtradeapi.un.org/bulk/v1/get/' + typeCode + '/' + freqCode + '/' + clCode
    else:
        if tradeDataType == 'TARIFFLINE':
            baseURL = 'https://comtradeapi.un.org/data/v1/getDaTariffline/' + typeCode + '/' + freqCode + '/' + clCode
        else:
            baseURL = 'https://comtradeapi.un.org/data/v1/getDa/' + typeCode + '/' + freqCode + '/' + clCode

    PARAMS = dict(reportercode=reporterCode, period=period, publishedDateFrom=publishedDateFrom, publishedDateTo=publishedDateTo)
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
            # print(df.head())
            # print(df.describe())
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

def getFinalDataAvailability(subscription_key, typeCode, freqCode, clCode, period, reporterCode, publishedDateFrom=None, publishedDateTo=None):
    return getDataAvailability(subscription_key, 'FINAL', None, typeCode, freqCode, clCode, period, reporterCode, publishedDateFrom, publishedDateTo)

def getTarifflineDataAvailability(subscription_key, typeCode, freqCode, clCode, period, reporterCode, publishedDateFrom=None, publishedDateTo=None):
    return getDataAvailability(subscription_key, 'TARIFFLINE', None, typeCode, freqCode, clCode, period, reporterCode, publishedDateFrom, publishedDateTo)

def getFinalDataBulkAvailability(subscription_key, typeCode, freqCode, clCode, period, reporterCode, publishedDateFrom=None, publishedDateTo=None):
    return getDataAvailability(subscription_key, 'FINAL', 'BULK', typeCode, freqCode, clCode, period, reporterCode, publishedDateFrom, publishedDateTo)

def getTarifflineDataBulkAvailability(subscription_key, typeCode, freqCode, clCode, period, reporterCode, publishedDateFrom=None, publishedDateTo=None):
    return getDataAvailability(subscription_key, 'TARIFFLINE', 'BULK', typeCode, freqCode, clCode, period, reporterCode, publishedDateFrom, publishedDateTo)
