import json
import pandas
from pandas import json_normalize
import requests

def getDataAvailability(subscription_key, tradeDataType, typeCode, freqCode, clCode, period, reporterCode):
    if tradeDataType == 'TARIFFLINE':
        baseURL = 'https://comtradeapi.un.org/data/v1/getDaTariffline/' + typeCode + '/' + freqCode + '/' + clCode
    else:
        baseURL = 'https://comtradeapi.un.org/data/v1/getDa/' + typeCode + '/' + freqCode + '/' + clCode

    PARAMS = dict(reportercode=reporterCode, period=period)
    # add key
    PARAMS["subscription-key"] = subscription_key
    # print(PARAMS)
    # only for JSON format
    if format_output != 'JSON':
        print("Only JSON output is supported with this function")
    else:
        try:
            resp = requests.get(baseURL, params=PARAMS, timeout=120)
            # print(resp.text)
            # print(resp.url)
            if resp.status_code != 200:
                # This means something went wrong.
                jsonResult = resp.json()
                print('Error in calling API:', resp.url)
                print('Error code:', jsonResult['statusCode'])
                print('Error message:', jsonResult['message'])
            else:
                jsonResult = resp.json()
                if countOnly:
                    dictCount = dict(count=jsonResult['count'])
                    df = pandas.DataFrame([dictCount])
                else:
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

def getFinalDataAvailability(subscription_key, typeCode, freqCode, clCode, period, reporterCode):
    return getDataAvailability(subscription_key, 'FINAL', typeCode, freqCode, clCode, period, reporterCode)

def getTarifflineDataAvailability(subscription_key, typeCode, freqCode, clCode, period, reporterCode):
    return getDataAvailability(subscription_key, 'TARIFFLINE', typeCode, freqCode, clCode, period, reporterCode)
