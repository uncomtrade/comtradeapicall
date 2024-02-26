import time as t
import pandas
import json
from pandas import json_normalize
import urllib3


def getPreviewData(subscription_key, tradeDataType, typeCode, freqCode, clCode, period, reporterCode, cmdCode,
                   flowCode,
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
    PARAMS["subscription-key"] = subscription_key
    fields = dict(filter(lambda item: item[1] is not None, PARAMS.items()))  
    http = urllib3.PoolManager()
    if format_output is None:
        format_output = 'JSON'
    if format_output != 'JSON':
        print("Only JSON output is supported with this function")
    else:
        try:
            resp = http.request("GET",baseURL, fields = fields, timeout=120)
            if resp.status != 200:
                print(resp.data.decode('utf-8'))
            else:
                jsonResult = json.loads(resp.data)
                if countOnly:
                    dictCount = dict(count=jsonResult['count'])
                    df = pandas.DataFrame([dictCount])
                else:
                    df = json_normalize(jsonResult['data'])  # Results contain the required data
                return df
        except urllib3.exceptions.RequestError as err:
            print(f'Request error: {err}')

def previewFinalData(typeCode, freqCode, clCode, period, reporterCode, cmdCode, flowCode,
                              partnerCode,
                              partner2Code, customsCode, motCode, maxRecords=None, format_output=None,
                            aggregateBy=None, breakdownMode=None, countOnly=None, includeDesc=None):
    return getPreviewData(None, 'FINAL', typeCode, freqCode, clCode, period, reporterCode,
                                  cmdCode, flowCode,
                                  partnerCode,
                                  partner2Code, customsCode, motCode, maxRecords, format_output, aggregateBy,
                                  breakdownMode,
                                  countOnly, includeDesc)

def _previewFinalData(typeCode, freqCode, clCode, period, reporterCode, cmdCode, flowCode,
                              partnerCode,
                              partner2Code, customsCode, motCode, maxRecords=None, format_output=None,
                            aggregateBy=None, breakdownMode=None, countOnly=None, includeDesc=None):
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
                              partner2Code, customsCode, motCode, maxRecords= None, format_output=None,
                              countOnly=None, includeDesc=None):
    return getPreviewData(None, 'TARIFFLINE', typeCode, freqCode, clCode, period, reporterCode,
                                  cmdCode, flowCode,
                                  partnerCode,
                                  partner2Code, customsCode, motCode, maxRecords, format_output, None,
                                  None,
                                  countOnly, includeDesc)

def _previewTarifflineData(typeCode, freqCode, clCode, period, reporterCode, cmdCode, flowCode,
                              partnerCode,
                              partner2Code, customsCode, motCode, maxRecords= None, format_output=None,
                              countOnly=None, includeDesc=None):
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
                              partner2Code, customsCode, motCode, maxRecords=None, format_output=None,
                            aggregateBy=None, breakdownMode=None, countOnly=None, includeDesc=None):
    return getPreviewData(subscription_key, 'FINAL', typeCode, freqCode, clCode, period, reporterCode,
                                  cmdCode, flowCode,
                                  partnerCode,
                                  partner2Code, customsCode, motCode, maxRecords, format_output, aggregateBy,
                                  breakdownMode,
                                  countOnly, includeDesc)

def _getFinalData(subscription_key, typeCode, freqCode, clCode, period, reporterCode, cmdCode, flowCode,
                              partnerCode,
                              partner2Code, customsCode, motCode, maxRecords=None, format_output=None,
                            aggregateBy=None, breakdownMode=None, countOnly=None, includeDesc=None):
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
                              partner2Code, customsCode, motCode, maxRecords= None, format_output=None,
                              countOnly=None, includeDesc=None):
    return getPreviewData(subscription_key, 'TARIFFLINE', typeCode, freqCode, clCode, period, reporterCode,
                                  cmdCode, flowCode,
                                  partnerCode,
                                  partner2Code, customsCode, motCode, maxRecords, format_output, None,
                                  None,
                                  countOnly, includeDesc)

def _getTarifflineData(subscription_key, typeCode, freqCode, clCode, period, reporterCode, cmdCode, flowCode,
                              partnerCode,
                              partner2Code, customsCode, motCode, maxRecords= None, format_output=None,
                              countOnly=None, includeDesc=None):
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

