import json
import time as t
from datetime import datetime
import pandas
from pandas import json_normalize
import requests
import os
from urllib.parse import urlparse


def submitAsyncDataRequest(subscription_key, endPoint, typeCode, freqCode, clCode, period, reporterCode, cmdCode,
                   flowCode, partnerCode, partner2Code, customsCode, motCode, aggregateBy, breakdownMode):
    if endPoint == 'TARIFFLINE':
        baseURL = 'https://comtradeapi.un.org/async/v1/getTariffline/' + typeCode + '/' + freqCode + '/' + clCode
    else:
        baseURL = 'https://comtradeapi.un.org/async/v1/get/' + typeCode + '/' + freqCode + '/' + clCode


    PARAMS = dict(reportercode=reporterCode, flowCode=flowCode,
                  period=period, cmdCode=cmdCode, partnerCode=partnerCode, partner2Code=partner2Code,
                  motCode=motCode, customsCode=customsCode, aggregateBy=aggregateBy, breakdownMode=breakdownMode)
    # add key
    PARAMS["subscription-key"] = subscription_key
    # print(PARAMS)
    try:
        resp = requests.get(baseURL, params=PARAMS, timeout=120)
        # print(resp.status_code)
        # print(resp.text)
        # print(resp.url)
        if resp.status_code != 202:
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
            print('Return message:', jsonResult['message'])
            return jsonResult
    except requests.exceptions.Timeout:
        # Maybe set up for a retry, or continue in a retry loop
        print('Request failed due to timeout')
    except requests.exceptions.TooManyRedirects:
        # Tell the user their URL was bad and try a different one
        print('Request failed due to too many redirects')
    except requests.exceptions.RequestException as e:
        # catastrophic error. bail.
        raise SystemExit(e)

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

def checkAsyncDataRequest(subscription_key, emailId, batchId=None):
    baseURL = 'https://comtradeapi.un.org/async/v1/getDA/'
    PARAMS = dict(batchId=batchId, emailId=emailId)
    # add key
    PARAMS["subscription-key"] = subscription_key
    # print(PARAMS)
    try:
        resp = requests.get(baseURL, params=PARAMS, timeout=120)
        # print(resp.status_code)
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

def downloadAsyncFinalDataRequest(subscription_key, directory, email, typeCode, freqCode, clCode, period,
                                  reporterCode, cmdCode, flowCode, partnerCode, partner2Code, customsCode, motCode,
                                  aggregateBy=None, breakdownMode=None):
    myJson = submitAsyncFinalDataRequest(subscription_key, typeCode, freqCode, clCode, period, reporterCode,
                                  cmdCode, flowCode,partnerCode,partner2Code, customsCode, motCode, aggregateBy, breakdownMode)
    batchId = myJson['requestId']
    # check status -- looping
    print("Processing and downloading the result. BatchId: ", batchId)
    status = ''
    while status != 'Completed' and status != 'Error':
        mydf = checkAsyncDataRequest(subscription_key, emailId=email, batchId=batchId)
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
        r = requests.get(url, stream=True)
        with open(download_path, "wb") as text:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    text.write(chunk)
        print(fileName, ' downloaded successfully')
    else:
        print('Error occurred when processing batchId: ', batchId)

def downloadAsyncTarifflineDataRequest(subscription_key, directory, email, typeCode, freqCode, clCode, period,
                                  reporterCode, cmdCode, flowCode, partnerCode, partner2Code, customsCode, motCode):
    myJson = submitAsyncTarifflineDataRequest(subscription_key, typeCode, freqCode, clCode, period, reporterCode,
                                  cmdCode, flowCode,partnerCode,partner2Code, customsCode, motCode)
    batchId = myJson['requestId']
    # check status -- looping
    print("Processing and downloading the result. BatchId: ", batchId)
    status = ''
    while status != 'Completed' and status != 'Error':
        mydf = checkAsyncDataRequest(subscription_key, emailId=email, batchId=batchId)
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
        r = requests.get(url, stream=True)
        with open(download_path, "wb") as text:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    text.write(chunk)
        print(fileName, ' downloaded successfully')
    else:
        print('Error occurred when processing batchId: ', batchId)