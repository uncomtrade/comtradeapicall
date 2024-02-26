import time as t
from pandas import json_normalize
import os
from urllib.parse import urlparse
import json
import urllib3
import shutil

def submitAsyncDataRequest(subscription_key, endPoint, typeCode, freqCode, clCode, period, reporterCode, cmdCode,
                   flowCode, partnerCode, partner2Code, customsCode, motCode, aggregateBy, breakdownMode):
    if endPoint == 'TARIFFLINE':
        baseURL = 'https://comtradeapi.un.org/async/v1/getTariffline/' + typeCode + '/' + freqCode + '/' + clCode
    else:
        baseURL = 'https://comtradeapi.un.org/async/v1/get/' + typeCode + '/' + freqCode + '/' + clCode

    PARAMS = dict(reportercode=reporterCode, flowCode=flowCode,
                  period=period, cmdCode=cmdCode, partnerCode=partnerCode, partner2Code=partner2Code,
                  motCode=motCode, customsCode=customsCode, aggregateBy=aggregateBy, breakdownMode=breakdownMode)
    PARAMS["subscription-key"] = subscription_key
    fields = dict(filter(lambda item: item[1] is not None, PARAMS.items()))  
    http = urllib3.PoolManager()
    try:
        resp = http.request("GET",baseURL, fields = fields, timeout=120)
        if resp.status != 202:
            print(resp.data.decode('utf-8'))
        else:
            jsonResult = json.loads(resp.data)
            print('Return message:', jsonResult['message'])
            return jsonResult
    except urllib3.exceptions.RequestError as err:
        print(f'Request error: {err}')

def submitAsyncFinalDataRequest(subscription_key, typeCode, freqCode, clCode, period, reporterCode, cmdCode, flowCode,
                              partnerCode,
                              partner2Code, customsCode, motCode,  aggregateBy=None,
                              breakdownMode=None):
    return submitAsyncDataRequest(subscription_key, 'FINAL', typeCode, freqCode, clCode, period, reporterCode,
                                  cmdCode, flowCode,
                                  partnerCode,
                                  partner2Code, customsCode, motCode, aggregateBy, breakdownMode)

def submitAsyncTarifflineDataRequest(subscription_key, typeCode, freqCode, clCode, period, reporterCode, cmdCode,
                                   flowCode,partnerCode, partner2Code, customsCode, motCode):
    return submitAsyncDataRequest(subscription_key, 'TARIFFLINE', typeCode, freqCode, clCode, period, reporterCode,
                                  cmdCode, flowCode,
                                  partnerCode,
                                  partner2Code, customsCode, motCode, None, None)

def checkAsyncDataRequest(subscription_key, batchId=None):
    baseURL = 'https://comtradeapi.un.org/async/v1/getDA/'
    PARAMS = dict(batchId=batchId)
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

def downloadAsyncFinalDataRequest(subscription_key, directory, typeCode, freqCode, clCode, period,
                                  reporterCode, cmdCode, flowCode, partnerCode, partner2Code, customsCode, motCode,
                                  aggregateBy=None, breakdownMode=None):
    myJson = submitAsyncFinalDataRequest(subscription_key, typeCode, freqCode, clCode, period, reporterCode,
                                  cmdCode, flowCode,partnerCode,partner2Code, customsCode, motCode, aggregateBy, breakdownMode)
    batchId = myJson['requestId']
    print("Processing and downloading the result. BatchId: ", batchId)
    status = ''
    while status != 'Completed' and status != 'Error':
        mydf = checkAsyncDataRequest(subscription_key, batchId=batchId)
        current_status = mydf.iloc[0]['status']
        if status != current_status:
            status = current_status
            print("Batch Status: ", current_status)
        t.sleep(15)
    if status == 'Completed':
        url = mydf.iloc[0]['uri']
        a = urlparse(url)
        fileName = os.path.basename(a.path)
        download_path = os.path.join(directory, fileName)
        #download file
        httpFILE = urllib3.PoolManager()
        with open(download_path, 'wb') as out:
            r = httpFILE.request('GET', url, preload_content=False)
            shutil.copyfileobj(r, out)
        r.release_conn()
        print(fileName, ' downloaded successfully')
    else:
        print('Error occurred when processing batchId: ', batchId)

def downloadAsyncTarifflineDataRequest(subscription_key, directory, typeCode, freqCode, clCode, period,
                                  reporterCode, cmdCode, flowCode, partnerCode, partner2Code, customsCode, motCode):
    myJson = submitAsyncTarifflineDataRequest(subscription_key, typeCode, freqCode, clCode, period, reporterCode,
                                  cmdCode, flowCode,partnerCode,partner2Code, customsCode, motCode)
    batchId = myJson['requestId']
    # check status -- looping
    print("Processing and downloading the result. BatchId: ", batchId)
    status = ''
    while status != 'Completed' and status != 'Error':
        mydf = checkAsyncDataRequest(subscription_key, batchId=batchId)
        current_status = mydf.iloc[0]['status']
        if status != current_status:
            status = current_status
            print("Batch Status: ", current_status)
        t.sleep(15)
    if status == 'Completed':
        url = mydf.iloc[0]['uri']
        a = urlparse(url)
        fileName = os.path.basename(a.path)
        download_path = os.path.join(directory, fileName)
        #download file
        httpFILE = urllib3.PoolManager()
        with open(download_path, 'wb') as out:
            r = httpFILE.request('GET', url, preload_content=False)
            shutil.copyfileobj(r, out)
        r.release_conn()
        print(fileName, ' downloaded successfully')
    else:
        print('Error occurred when processing batchId: ', batchId)