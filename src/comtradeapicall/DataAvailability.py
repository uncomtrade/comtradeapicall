import json
from pandas import json_normalize
import urllib3

def getLiveUpdate(subscription_key):
    baseURL = 'https://comtradeapi.un.org/data/v1/getLiveUpdate'
    PARAMS = dict()
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
            return df
    except urllib3.exceptions.RequestError as err:
        print(f'Request error: {err}')

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
    PARAMS["subscription-key"] = subscription_key
    fields = dict(filter(lambda item: item[1] is not None, PARAMS.items()))  
    http = urllib3.PoolManager()
    try:
        resp = http.request("GET", baseURL, fields = fields, timeout=120)
        if resp.status != 200:
            print(resp.data.decode('utf-8'))
        else:
            jsonResult = json.loads(resp.data)
            df = json_normalize(jsonResult['data'])
            return df
    except urllib3.exceptions.RequestError as err:
        print(f'Request error: {err}')

def getFinalDataAvailability(subscription_key, typeCode, freqCode, clCode, period, reporterCode, publishedDateFrom=None, publishedDateTo=None):
    return getDataAvailability(subscription_key, 'FINAL', None, typeCode, freqCode, clCode, period, reporterCode, publishedDateFrom, publishedDateTo)

def getTarifflineDataAvailability(subscription_key, typeCode, freqCode, clCode, period, reporterCode, publishedDateFrom=None, publishedDateTo=None):
    return getDataAvailability(subscription_key, 'TARIFFLINE', None, typeCode, freqCode, clCode, period, reporterCode, publishedDateFrom, publishedDateTo)

def getFinalDataBulkAvailability(subscription_key, typeCode, freqCode, clCode, period, reporterCode, publishedDateFrom=None, publishedDateTo=None):
    return getDataAvailability(subscription_key, 'FINAL', 'BULK', typeCode, freqCode, clCode, period, reporterCode, publishedDateFrom, publishedDateTo)

def getTarifflineDataBulkAvailability(subscription_key, typeCode, freqCode, clCode, period, reporterCode, publishedDateFrom=None, publishedDateTo=None):
    return getDataAvailability(subscription_key, 'TARIFFLINE', 'BULK', typeCode, freqCode, clCode, period, reporterCode, publishedDateFrom, publishedDateTo)
