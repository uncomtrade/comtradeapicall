import json
from pandas import json_normalize
import urllib3


def getSUV(subscription_key, typeCode='C', freqCode='A', clCode='HS', period=None, cmdCode=None,
           flowCode=None, qtyUnitCode=None, proxy_url=None):
    baseURL = 'https://comtradeapi.un.org/data/v1/getSUV/' + \
        typeCode + '/' + freqCode + '/' + clCode

    PARAMS = dict(flowCode=flowCode, period=period,
                  cmdCode=cmdCode, qtyUnitCode=qtyUnitCode)
    PARAMS["subscription-key"] = subscription_key
    fields = dict(filter(lambda item: item[1] is not None, PARAMS.items()))
    if proxy_url:
        http = urllib3.ProxyManager(proxy_url=proxy_url)
    else:
        http = urllib3.PoolManager()
    try:
        resp = http.request("GET", baseURL, fields=fields, timeout=120)
        if resp.status != 200:
            print(resp.data)
        else:
            jsonResult = json.loads(resp.data)
            df = json_normalize(jsonResult['data'])
            return df
    except urllib3.exceptions.RequestError as err:
        print(f'Request error: {err}')
