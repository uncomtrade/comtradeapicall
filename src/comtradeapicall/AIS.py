import json
from pandas import json_normalize
import urllib3


def getAIS(subscription_key, typeCode='C', freqCode='D', clCode='XX', countryareaCode='', vesselTypeCode='', flowCode='', dateFrom='', dateTo='', maxRecords=250000, proxy_url=None):
    baseURL = 'https://comtradeapi.un.org/experimental/v1/getAIS/' + \
        typeCode + '/' + freqCode + '/' + clCode
    PARAMS = dict(countryareaCode=countryareaCode, vesselTypeCode=vesselTypeCode,
                  flowCode=flowCode, dateFrom=dateFrom, dateTo=dateTo,
                  maxRecords=maxRecords)
    PARAMS["subscription-key"] = subscription_key
    fields = dict(filter(lambda item: item[1] is not None, PARAMS.items()))
    if proxy_url:
        http = urllib3.ProxyManager(proxy_url=proxy_url)
    else:
        http = urllib3.PoolManager()
    try:
        resp = http.request("GET", baseURL, fields=fields, timeout=120)
        if resp.status != 200:
            print(resp.data.decode('utf-8'))
        else:
            jsonResult = json.loads(resp.data)
            df = json_normalize(jsonResult['data'])
            return df
    except urllib3.exceptions.RequestError as err:
        print(f'Request error: {err}')
