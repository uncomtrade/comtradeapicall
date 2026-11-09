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
                   countOnly, includeDesc, proxy_url):
    if subscription_key is not None:
        if tradeDataType == 'TARIFFLINE':
            baseURL = 'https://comtradeapi.un.org/data/v1/getTariffline/' + \
                typeCode + '/' + freqCode + '/' + clCode
        elif tradeDataType == 'TRADEMATRIX':
            # override any given clCode
            clCode = "TM"
            baseURL = 'https://comtradeapi.un.org/data/v1/getTradeMatrix/' + \
                typeCode + '/' + freqCode + '/' + clCode
        else:
            baseURL = 'https://comtradeapi.un.org/data/v1/get/' + \
                typeCode + '/' + freqCode + '/' + clCode
    else:
        if tradeDataType == 'TARIFFLINE':
            baseURL = 'https://comtradeapi.un.org/public/v1/previewTariffline/' + \
                typeCode + '/' + freqCode + '/' + clCode
        else:
            baseURL = 'https://comtradeapi.un.org/public/v1/preview/' + \
                typeCode + '/' + freqCode + '/' + clCode

    PARAMS = dict(reportercode=reporterCode, flowCode=flowCode,
                  period=period, cmdCode=cmdCode, partnerCode=partnerCode, partner2Code=partner2Code,
                  motCode=motCode, customsCode=customsCode,
                  maxRecords=maxRecords, format=format_output, aggregateBy=aggregateBy, breakdownMode=breakdownMode,
                  countOnly=countOnly, includeDesc=includeDesc)
    PARAMS["subscription-key"] = subscription_key
    fields = dict(filter(lambda item: item[1] is not None, PARAMS.items()))
    if proxy_url:
        http = urllib3.ProxyManager(proxy_url=proxy_url)
    else:
        http = urllib3.PoolManager()
    if format_output is None:
        format_output = 'JSON'
    if format_output != 'JSON':
        print("Only JSON output is supported with this function")
    else:
        try:
            resp = http.request("GET", baseURL, fields=fields, timeout=120)
            if resp.status != 200:
                raise RuntimeError(f"HTTP {resp.status}: {resp.data.decode('utf-8', errors='replace')}")
            else:
                jsonResult = json.loads(resp.data)
                if countOnly:
                    dictCount = dict(count=jsonResult['count'])
                    df = pandas.DataFrame([dictCount])
                else:
                    # Results contain the required data
                    df = json_normalize(jsonResult['data'])
                return df
        except urllib3.exceptions.HTTPError as err:
            raise


def previewFinalData(typeCode, freqCode, clCode, period, reporterCode, cmdCode, flowCode,
                     partnerCode,
                     partner2Code, customsCode, motCode, maxRecords=None, format_output=None,
                     aggregateBy=None, breakdownMode=None, countOnly=None, includeDesc=None, proxy_url=None):
    return getPreviewData(None, 'FINAL', typeCode, freqCode, clCode, period, reporterCode,
                          cmdCode, flowCode,
                          partnerCode,
                          partner2Code, customsCode, motCode, maxRecords, format_output, aggregateBy,
                          breakdownMode,
                          countOnly, includeDesc, proxy_url)


def previewCountFinalData(typeCode, freqCode, clCode, period, reporterCode, cmdCode, flowCode,
                          partnerCode,
                          partner2Code, customsCode, motCode, aggregateBy=None, breakdownMode=None, proxy_url=None):
    return getPreviewData(None, 'FINAL', typeCode, freqCode, clCode, period, reporterCode,
                          cmdCode, flowCode,
                          partnerCode,
                          partner2Code, customsCode, motCode, maxRecords=None, format_output=None, aggregateBy=aggregateBy,
                          breakdownMode=breakdownMode,
                          countOnly=True, includeDesc=None, proxy_url=proxy_url)


def getCountFinalData(subscription_key, typeCode, freqCode, clCode, period, reporterCode, cmdCode, flowCode,
                      partnerCode,
                      partner2Code, customsCode, motCode, aggregateBy=None, breakdownMode=None, proxy_url=None):
    return getFinalData(subscription_key, typeCode, freqCode, clCode, period, reporterCode,
                        cmdCode, flowCode,
                        partnerCode,
                        partner2Code, customsCode, motCode, maxRecords=None, format_output=None, aggregateBy=aggregateBy,
                        breakdownMode=breakdownMode,
                        countOnly=True, includeDesc=None, proxy_url=proxy_url)


def _previewFinalData(typeCode, freqCode, clCode, period, reporterCode, cmdCode, flowCode,
                      partnerCode,
                      partner2Code, customsCode, motCode, maxRecords=None, format_output=None,
                      aggregateBy=None, breakdownMode=None, countOnly=None, includeDesc=None, proxy_url=None):
    main_df = pandas.DataFrame()
    for single_period in list(period.split(",")):
        try:
            staging_df = previewFinalData(typeCode, freqCode, clCode, single_period, reporterCode, cmdCode, flowCode,
                                          partnerCode,
                                          partner2Code, customsCode, motCode, maxRecords, format_output, aggregateBy,
                                          breakdownMode,
                                          countOnly, includeDesc, proxy_url)
        except urllib3.exceptions.HTTPError:  # retry transport failures only
            print('Repeating API call for period: ' + single_period)
            t.sleep(10)
            staging_df = previewFinalData(typeCode, freqCode, clCode, single_period, reporterCode, cmdCode, flowCode,
                                          partnerCode,
                                          partner2Code, customsCode, motCode, maxRecords, format_output, aggregateBy,
                                          breakdownMode,
                                          countOnly, includeDesc, proxy_url)
        main_df = pandas.concat([main_df, staging_df])
    return main_df


def previewTarifflineData(typeCode, freqCode, clCode, period, reporterCode, cmdCode, flowCode,
                          partnerCode,
                          partner2Code, customsCode, motCode, maxRecords=None, format_output=None,
                          countOnly=None, includeDesc=None, proxy_url=None):
    return getPreviewData(None, 'TARIFFLINE', typeCode, freqCode, clCode, period, reporterCode,
                          cmdCode, flowCode,
                          partnerCode,
                          partner2Code, customsCode, motCode, maxRecords, format_output, None,
                          None,
                          countOnly, includeDesc, proxy_url)


def _previewTarifflineData(typeCode, freqCode, clCode, period, reporterCode, cmdCode, flowCode,
                           partnerCode,
                           partner2Code, customsCode, motCode, maxRecords=None, format_output=None,
                           countOnly=None, includeDesc=None, proxy_url=None):
    main_df = pandas.DataFrame()
    for single_period in list(period.split(",")):
        try:
            staging_df = previewTarifflineData(typeCode, freqCode, clCode, single_period, reporterCode, cmdCode,
                                               flowCode,
                                               partnerCode,
                                               partner2Code, customsCode, motCode, maxRecords, format_output,
                                               countOnly, includeDesc, proxy_url)
        except urllib3.exceptions.HTTPError:  # retry transport failures only
            print('Repeating API call for period: ' + single_period)
            t.sleep(10)
            staging_df = previewTarifflineData(typeCode, freqCode, clCode, single_period, reporterCode, cmdCode, flowCode,
                                               partnerCode,
                                               partner2Code, customsCode, motCode, maxRecords, format_output,
                                               countOnly, includeDesc, proxy_url)
        main_df = pandas.concat([main_df, staging_df])
    return main_df


def getFinalData(subscription_key, typeCode, freqCode, clCode, period, reporterCode, cmdCode, flowCode,
                 partnerCode,
                 partner2Code, customsCode, motCode, maxRecords=None, format_output=None,
                 aggregateBy=None, breakdownMode=None, countOnly=None, includeDesc=None, proxy_url=None):
    return getPreviewData(subscription_key, 'FINAL', typeCode, freqCode, clCode, period, reporterCode,
                          cmdCode, flowCode,
                          partnerCode,
                          partner2Code, customsCode, motCode, maxRecords, format_output, aggregateBy,
                          breakdownMode,
                          countOnly, includeDesc, proxy_url)


def _getFinalData(subscription_key, typeCode, freqCode, clCode, period, reporterCode, cmdCode, flowCode,
                  partnerCode,
                  partner2Code, customsCode, motCode, maxRecords=None, format_output=None,
                  aggregateBy=None, breakdownMode=None, countOnly=None, includeDesc=None, proxy_url=None):
    main_df = pandas.DataFrame()
    for single_period in list(period.split(",")):
        try:
            staging_df = getFinalData(subscription_key, typeCode, freqCode, clCode, single_period, reporterCode,
                                      cmdCode,
                                      flowCode, partnerCode,
                                      partner2Code, customsCode, motCode, maxRecords, format_output, aggregateBy,
                                      breakdownMode,
                                      countOnly, includeDesc, proxy_url)
        except urllib3.exceptions.HTTPError:  # retry transport failures only
            print('Repeating API call for period: ' + single_period)
            t.sleep(10)
            staging_df = getFinalData(subscription_key, typeCode, freqCode, clCode, single_period, reporterCode, cmdCode,
                                      flowCode, partnerCode,
                                      partner2Code, customsCode, motCode, maxRecords, format_output, aggregateBy,
                                      breakdownMode,
                                      countOnly, includeDesc, proxy_url)
        main_df = pandas.concat([main_df, staging_df])
    return main_df


def getTarifflineData(subscription_key, typeCode, freqCode, clCode, period, reporterCode, cmdCode, flowCode,
                      partnerCode,
                      partner2Code, customsCode, motCode, maxRecords=None, format_output=None,
                      countOnly=None, includeDesc=None, proxy_url=None):
    return getPreviewData(subscription_key, 'TARIFFLINE', typeCode, freqCode, clCode, period, reporterCode,
                          cmdCode, flowCode,
                          partnerCode,
                          partner2Code, customsCode, motCode, maxRecords, format_output, None,
                          None,
                          countOnly, includeDesc, proxy_url)


def _getTarifflineData(subscription_key, typeCode, freqCode, clCode, period, reporterCode, cmdCode, flowCode,
                       partnerCode,
                       partner2Code, customsCode, motCode, maxRecords=None, format_output=None,
                       countOnly=None, includeDesc=None, proxy_url=None):
    main_df = pandas.DataFrame()
    for single_period in list(period.split(",")):
        try:
            staging_df = getTarifflineData(subscription_key, typeCode, freqCode, clCode, single_period, reporterCode,
                                           cmdCode, flowCode, partnerCode,
                                           partner2Code, customsCode, motCode, maxRecords, format_output,
                                           countOnly, includeDesc, proxy_url)
        except urllib3.exceptions.HTTPError:  # retry transport failures only
            print('Repeating API call for period: ' + single_period)
            t.sleep(10)
            staging_df = getTarifflineData(subscription_key, typeCode, freqCode, clCode, single_period, reporterCode,
                                           cmdCode, flowCode, partnerCode,
                                           partner2Code, customsCode, motCode, maxRecords, format_output,
                                           countOnly, includeDesc, proxy_url)
        main_df = pandas.concat([main_df, staging_df])
    return main_df


def getTradeBalance(subscription_key, typeCode, freqCode, clCode, period, reporterCode, cmdCode,
                    partnerCode,
                    partner2Code=None, customsCode=None, motCode=None, maxRecords=None, format_output='JSON',
                    breakdownMode='classic',
                    includeDesc=None, proxy_url=None):

    baseURL = 'https://comtradeapi.un.org/tools/v1/getTradeBalance/' + \
        typeCode + '/' + freqCode + '/' + clCode

    PARAMS = dict(reportercode=reporterCode, period=period, cmdCode=cmdCode, partnerCode=partnerCode, partner2Code=partner2Code,
                  motCode=motCode, customsCode=customsCode,
                  maxRecords=maxRecords, format=format_output,  breakdownMode=breakdownMode,
                  includeDesc=includeDesc)
    PARAMS["subscription-key"] = subscription_key
    fields = dict(filter(lambda item: item[1] is not None, PARAMS.items()))
    if proxy_url:
        http = urllib3.ProxyManager(proxy_url=proxy_url)
    else:
        http = urllib3.PoolManager()
    if format_output is None:
        format_output = 'JSON'
    if format_output != 'JSON':
        print("Only JSON output is supported with this function")
    else:
        try:
            resp = http.request("GET", baseURL, fields=fields, timeout=120)
            if resp.status != 200:
                raise RuntimeError(f"HTTP {resp.status}: {resp.data.decode('utf-8', errors='replace')}")
            else:
                jsonResult = json.loads(resp.data)
                # Results contain the required data
                df = json_normalize(jsonResult['data'])
                return df
        except urllib3.exceptions.HTTPError as err:
            raise


def getBilateralData(subscription_key, typeCode, freqCode, clCode, period, reporterCode, cmdCode, flowCode,
                     partnerCode,
                     maxRecords=None, format_output='JSON',
                     includeDesc=None, proxy_url=None):

    baseURL = 'https://comtradeapi.un.org/tools/v1/getBilateralData/' + \
        typeCode + '/' + freqCode + '/' + clCode

    PARAMS = dict(reportercode=reporterCode, period=period, cmdCode=cmdCode, flowCode=flowCode, partnerCode=partnerCode,
                  maxRecords=maxRecords, format=format_output,
                  includeDesc=includeDesc)
    PARAMS["subscription-key"] = subscription_key
    fields = dict(filter(lambda item: item[1] is not None, PARAMS.items()))
    if proxy_url:
        http = urllib3.ProxyManager(proxy_url=proxy_url)
    else:
        http = urllib3.PoolManager()
    if format_output is None:
        format_output = 'JSON'
    if format_output != 'JSON':
        print("Only JSON output is supported with this function")
    else:
        try:
            resp = http.request("GET", baseURL, fields=fields, timeout=120)
            if resp.status != 200:
                raise RuntimeError(f"HTTP {resp.status}: {resp.data.decode('utf-8', errors='replace')}")
            else:
                jsonResult = json.loads(resp.data)
                # Results contain the required data
                df = json_normalize(jsonResult['data'])
                return df
        except urllib3.exceptions.HTTPError as err:
            raise


def getTradeMatrix(subscription_key, typeCode, freqCode, period, reporterCode, cmdCode, flowCode,
                   partnerCode,
                   maxRecords=None, format_output=None,
                   aggregateBy=None, countOnly=None, includeDesc=None, proxy_url=None):
    return getPreviewData(subscription_key, 'TRADEMATRIX', typeCode, freqCode, clCode='TM', period=period, reporterCode=reporterCode,
                          cmdCode=cmdCode, flowCode=flowCode,
                          partnerCode=partnerCode,
                          partner2Code=None, customsCode=None, motCode=None, maxRecords=maxRecords, format_output=format_output, aggregateBy=aggregateBy,
                          breakdownMode='classic',
                          countOnly=countOnly, includeDesc=includeDesc, proxy_url=proxy_url)


def getWorldShare(typeCode, freqCode, period=None, reporterCode=None, proxy_url=None):
    """Retrieve public world-share data as a pandas DataFrame.

    typeCode and freqCode form the URL path. period and reporterCode are
    optional query filters. No subscription key is required.
    """
    baseURL = 'https://comtradeapi.un.org/public/v1/getWorldShare/' + typeCode + '/' + freqCode
    params = dict(period=period, reporterCode=reporterCode)
    fields = {key: value for key, value in params.items() if value is not None}
    http = urllib3.ProxyManager(proxy_url=proxy_url) if proxy_url else urllib3.PoolManager()
    try:
        resp = http.request('GET', baseURL, fields=fields, timeout=120)
        if resp.status != 200:
            print(resp.data.decode('utf-8', errors='replace'))
            return None
        payload = json.loads(resp.data)
        # Accept common API response envelopes or a direct JSON record list.
        if isinstance(payload, dict):
            if 'data' in payload:
                records = payload['data']
            elif 'results' in payload:
                records = payload['results']
            else:
                raise ValueError('World-share response is missing data or results')
        elif isinstance(payload, list):
            records = payload
        else:
            raise ValueError('Unexpected world-share response format')
        return json_normalize(records if records is not None else [])
    except urllib3.exceptions.RequestError as err:
        print(f'Request error: {err}')


def getMBS(series_type, subscription_key=None, year=None, country_code=None,
           period=None, period_type=None, table_type=None, format_output='json',
           proxy_url=None):
    """Retrieve Monthly Bulletin of Statistics data from the public/data API.

    series_type is required. A non-None subscription_key selects the data
    endpoint; otherwise the public endpoint is used. JSON and CSV return a
    pandas DataFrame; TXT returns decoded text. Optional filters are omitted
    when None. format_output maps to the API query parameter 'format'.
    """
    if not series_type:
        raise ValueError('series_type is required')
    if subscription_key is not None:
        baseURL = 'https://comtradeapi.un.org/data/v1/getMBS'
    else:
        baseURL = 'https://comtradeapi.un.org/public/v1/getMBS'

    format_output = 'json' if format_output is None else str(format_output).lower()
    if format_output not in ('json', 'csv', 'txt'):
        raise ValueError('format_output must be json, csv or txt')
    PARAMS = dict(series_type=series_type, year=year, country_code=country_code,
                  period=period, period_type=period_type, table_type=table_type,
                  format=format_output)
    PARAMS['subscription-key'] = subscription_key
    fields = dict(filter(lambda item: item[1] is not None, PARAMS.items()))
    if proxy_url:
        http = urllib3.ProxyManager(proxy_url=proxy_url)
    else:
        http = urllib3.PoolManager()
    try:
        resp = http.request('GET', baseURL, fields=fields, timeout=120)
        if resp.status != 200:
            print(resp.data.decode('utf-8', errors='replace'))
            return None
        if format_output == 'txt':
            return resp.data.decode('utf-8-sig')
        if format_output == 'csv':
            from io import StringIO
            text = resp.data.decode('utf-8-sig')
            return pandas.read_csv(StringIO(text)) if text.strip() else pandas.DataFrame()
        jsonResult = json.loads(resp.data)
        if isinstance(jsonResult, dict):
            if 'data' in jsonResult:
                records = jsonResult['data']
            elif 'results' in jsonResult:
                records = jsonResult['results']
            else:
                raise ValueError('MBS JSON response is missing data or results')
        elif isinstance(jsonResult, list):
            records = jsonResult
        else:
            raise ValueError('Unexpected MBS JSON response format')
        return json_normalize(records if records is not None else [])
    except urllib3.exceptions.RequestError as err:
        print(f'Request error: {err}')