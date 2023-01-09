import json
import os
import pandas
from pandas import json_normalize
import requests
import shutil
import gzip

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

def bulkDownloadFile(subscription_key, directory, tradeDataType, typeCode, freqCode, clCode, period, reporterCode, decompress):

    if tradeDataType == 'TARIFFLINE':
        baseURLDataAvailability = 'https://comtradeapi.un.org/bulk/v1/getTariffline/' + typeCode + '/' + freqCode + '/' + clCode
        prefixFile = 'TARIFFLINE'
    else:
        baseURLDataAvailability = 'https://comtradeapi.un.org/bulk/v1/get/' + typeCode + '/' + freqCode + '/' + clCode
        prefixFile = 'FINAL'

    PARAMS = dict(reportercode=reporterCode, period=period)
    # add key
    PARAMS["subscription-key"] = subscription_key
    try:
        resp = requests.get(baseURLDataAvailability, params=PARAMS, timeout=120)
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
            if jsonResult['count'] == 0:
                print('No data available based on the selection criteria')
            # if data available then download
            else:
                df = json_normalize(jsonResult['data'])  # Results contain the required data
                # print(df.head())
                # print(df.describe())
                totalFiles = df[df.columns[0]].count()
                i = 0
                while i < totalFiles:
                    file_url = df.fileUrl[i]
                    # print(file_url)
                    PARAMS = dict()
                    PARAMS["subscription-key"] = subscription_key
                    r = requests.get(file_url, params=PARAMS, stream=True)
                    if tradeDataType == 'TARIFFLINE':
                        fileName = "COMTRADE-" + prefixFile + "-" + df.typeCode[i] + df.freqCode[i] + str(df.reporterCode[i]).zfill(
                            3) + str(df.period[i]) + df.classificationCode[i] + "[" + df.timestamp[i][
                                                                                                :10] + "].gz"
                    else:
                        fileName = "COMTRADE-" + prefixFile + "-" + df.typeCode[i] + df.freqCode[i] + str(
                            df.reporterCode[i]).zfill(
                            3) + str(df.period[i]) + df.classificationCode[i] + "[" + df.publicationDate[i][
                                                                                      :10] + "].gz"
                    download_path = os.path.join(directory, fileName)
                    with open(download_path, "wb") as text:
                        for chunk in r.iter_content(chunk_size=1024):
                            # writing one chunk at a time to pdf file
                            if chunk:
                                text.write(chunk)
                    print(fileName.replace(".gz", "") + ' downloaded')
                    i = i + 1
                    download_path_gunzip = download_path.replace(".gz", ".txt")
                    if decompress is True:
                        with gzip.open(download_path, "rb")  as f_in:
                            with open(download_path_gunzip, 'wb') as f_out:
                                shutil.copyfileobj(f_in, f_out)
                        os.remove(download_path)
                print('Total of ' + str(i) + ' file(s) downloaded')
    except requests.exceptions.Timeout:
        # Maybe set up for a retry, or continue in a retry loop
        print('Request failed due to timeout')
    except requests.exceptions.TooManyRedirects:
        # Tell the user their URL was bad and try a different one
        print('Request failed due to too many redirects')
    except requests.exceptions.RequestException as e:
        # catastrophic error. bail.
        raise SystemExit(e)

def bulkDownloadFinalFile(subscription_key, directory, typeCode, freqCode, clCode, period, reporterCode, decompress):
    bulkDownloadFile(subscription_key, directory, 'FINAL', typeCode, freqCode, clCode, period,
                             reporterCode, decompress)

def bulkDownloadTarifflineFile(subscription_key, directory, typeCode, freqCode, clCode, period, reporterCode, decompress):
    bulkDownloadFile(subscription_key, directory, 'TARIFFLINE', typeCode, freqCode, clCode, period,
                             reporterCode, decompress)

