import json
import time as t
from datetime import datetime
import pandas
from pandas import json_normalize
import requests

def getPreviewData(subscription_key, tradeDataType, typeCode, freqCode, clCode, period, reporterCode, cmdCode, flowCode,
                              partnerCode,
                              partner2Code, customsCode, motCode, maxRecords, format_output, aggregateBy,
                              breakdownMode,
                              countOnly, includeDesc):
    if subscription_key is not None:
        if tradeDataType == 'TARIFFLINE':
            baseURL = 'https://comtradeapi.un.org/data/v1/getTariffline/' + typeCode + '/' + freqCode + '/' + clCode
        else:
            baseURL = 'https://comtradeapi.un.org/data/v1/get/' + typeCode + '/' + freqCode + '/' + clCode
    else:
        if tradeDataType == 'TARIFFLINE':
            baseURL = 'https://comtradeapi.un.org/public/v1/previewTariffline/' + typeCode + '/' + freqCode + '/' + clCode
        else:
            baseURL = 'https://comtradeapi.un.org/public/v1/preview/' + typeCode + '/' + freqCode + '/' + clCode

    PARAMS = dict(reportercode=reporterCode, flowCode=flowCode,
                  period=period, cmdCode=cmdCode, partnerCode=partnerCode, partner2Code=partner2Code,
                  motCode=motCode, customsCode=customsCode,
                  maxRecords=maxRecords, format=format_output, aggregateBy=aggregateBy, breakdownMode=breakdownMode,
                  countOnly=countOnly, includeDesc=includeDesc)
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

def previewFinalData(typeCode, freqCode, clCode, period, reporterCode, cmdCode, flowCode,
                              partnerCode,
                              partner2Code, customsCode, motCode, maxRecords, format_output, aggregateBy,
                              breakdownMode,
                              countOnly, includeDesc):
    return getPreviewData(None, 'FINAL', typeCode, freqCode, clCode, period, reporterCode,
                                  cmdCode, flowCode,
                                  partnerCode,
                                  partner2Code, customsCode, motCode, maxRecords, format_output, aggregateBy,
                                  breakdownMode,
                                  countOnly, includeDesc)

def _previewFinalData(typeCode, freqCode, clCode, period, reporterCode, cmdCode, flowCode,
                              partnerCode,
                              partner2Code, customsCode, motCode, maxRecords, format_output, aggregateBy,
                              breakdownMode,
                                countOnly, includeDesc):
    main_df = pandas.DataFrame()
    for single_period in list(period.split(",")):
        try:
            staging_df = previewFinalData(typeCode, freqCode, clCode, single_period, reporterCode, cmdCode, flowCode,
                                          partnerCode,
                                          partner2Code, customsCode, motCode, maxRecords, format_output, aggregateBy,
                                          breakdownMode,
                                          countOnly, includeDesc)
        except: #retry once more after 10 secs
            print('Repeating API call for period: ' + single_period)
            t.sleep(10)
            staging_df = previewFinalData(typeCode, freqCode, clCode, single_period, reporterCode, cmdCode, flowCode,
                     partnerCode,
                     partner2Code, customsCode, motCode, maxRecords, format_output, aggregateBy,
                     breakdownMode,
                     countOnly, includeDesc)
        main_df = pandas.concat([main_df, staging_df])
    return main_df


def previewTarifflineData(typeCode, freqCode, clCode, period, reporterCode, cmdCode, flowCode,
                              partnerCode,
                              partner2Code, customsCode, motCode, maxRecords, format_output,
                              countOnly, includeDesc):
    return getPreviewData(None, 'TARIFFLINE', typeCode, freqCode, clCode, period, reporterCode,
                                  cmdCode, flowCode,
                                  partnerCode,
                                  partner2Code, customsCode, motCode, maxRecords, format_output, None,
                                  None,
                                  countOnly, includeDesc)

def _previewTarifflineData(typeCode, freqCode, clCode, period, reporterCode, cmdCode, flowCode,
                              partnerCode,
                              partner2Code, customsCode, motCode, maxRecords, format_output,
                              countOnly, includeDesc):
    main_df = pandas.DataFrame()
    for single_period in list(period.split(",")):
        try:
            staging_df = previewTarifflineData(typeCode, freqCode, clCode, single_period, reporterCode, cmdCode,
                                               flowCode,
                                               partnerCode,
                                               partner2Code, customsCode, motCode, maxRecords, format_output,
                                               countOnly, includeDesc)
        except: #retry once more after 10 secs
            print('Repeating API call for period: ' + single_period)
            t.sleep(10)
            staging_df = previewTarifflineData(typeCode, freqCode, clCode, single_period, reporterCode, cmdCode, flowCode,
                              partnerCode,
                              partner2Code, customsCode, motCode, maxRecords, format_output,
                              countOnly, includeDesc)
        main_df = pandas.concat([main_df, staging_df])
    return main_df

def getFinalData(subscription_key, typeCode, freqCode, clCode, period, reporterCode, cmdCode, flowCode,
                              partnerCode,
                              partner2Code, customsCode, motCode, maxRecords, format_output, aggregateBy,
                              breakdownMode,
                              countOnly, includeDesc):
    return getPreviewData(subscription_key, 'FINAL', typeCode, freqCode, clCode, period, reporterCode,
                                  cmdCode, flowCode,
                                  partnerCode,
                                  partner2Code, customsCode, motCode, maxRecords, format_output, aggregateBy,
                                  breakdownMode,
                                  countOnly, includeDesc)

def _getFinalData(subscription_key, typeCode, freqCode, clCode, period, reporterCode, cmdCode, flowCode,
                              partnerCode,
                              partner2Code, customsCode, motCode, maxRecords, format_output, aggregateBy,
                              breakdownMode,
                              countOnly, includeDesc):
    main_df = pandas.DataFrame()
    for single_period in list(period.split(",")):
        try:
            staging_df = getFinalData(subscription_key, typeCode, freqCode, clCode, single_period, reporterCode,
                                      cmdCode,
                                      flowCode, partnerCode,
                                      partner2Code, customsCode, motCode, maxRecords, format_output, aggregateBy,
                                      breakdownMode,
                                      countOnly, includeDesc)
        except: #retry once more after 10 secs
            print('Repeating API call for period: ' + single_period)
            t.sleep(10)
            staging_df = getFinalData(subscription_key, typeCode, freqCode, clCode, single_period, reporterCode, cmdCode,
                                  flowCode, partnerCode,
                                  partner2Code, customsCode, motCode, maxRecords, format_output, aggregateBy,
                                  breakdownMode,
                                  countOnly, includeDesc)
        main_df = pandas.concat([main_df, staging_df])
    return main_df

def getTarifflineData(subscription_key, typeCode, freqCode, clCode, period, reporterCode, cmdCode, flowCode,
                              partnerCode,
                              partner2Code, customsCode, motCode, maxRecords, format_output,
                              countOnly, includeDesc):
    return getPreviewData(subscription_key, 'TARIFFLINE', typeCode, freqCode, clCode, period, reporterCode,
                                  cmdCode, flowCode,
                                  partnerCode,
                                  partner2Code, customsCode, motCode, maxRecords, format_output, None,
                                  None,
                                  countOnly, includeDesc)

def _getTarifflineData(subscription_key, typeCode, freqCode, clCode, period, reporterCode, cmdCode, flowCode,
                              partnerCode,
                              partner2Code, customsCode, motCode, maxRecords, format_output,
                              countOnly, includeDesc):
    main_df = pandas.DataFrame()
    for single_period in list(period.split(",")):
        try:
            staging_df = getTarifflineData(subscription_key, typeCode, freqCode, clCode, single_period, reporterCode,
                              cmdCode, flowCode, partnerCode,
                              partner2Code, customsCode, motCode, maxRecords, format_output,
                              countOnly, includeDesc)
        except: #retry once more after 10 secs
            print('Repeating API call for period: ' + single_period)
            t.sleep(10)
            staging_df = getTarifflineData(subscription_key, typeCode, freqCode, clCode, single_period, reporterCode,
                              cmdCode, flowCode, partnerCode,
                              partner2Code, customsCode, motCode, maxRecords, format_output,
                              countOnly, includeDesc)
        main_df = pandas.concat([main_df, staging_df])
    return main_df