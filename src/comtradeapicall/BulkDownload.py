import os
import requests
import shutil
import gzip
from pandas import json_normalize

def bulkDownloadFile(subscription_key, directory, tradeDataType, typeCode, freqCode, clCode, period, reporterCode,
                     decompress, publishedDateFrom=None, publishedDateTo=None):

    if tradeDataType == 'TARIFFLINE':
        baseURLDataAvailability = 'https://comtradeapi.un.org/bulk/v1/getTariffline/' + typeCode + '/' + freqCode + '/' + clCode
        prefixFile = 'TARIFFLINE'
    else:
        baseURLDataAvailability = 'https://comtradeapi.un.org/bulk/v1/get/' + typeCode + '/' + freqCode + '/' + clCode
        prefixFile = 'FINAL'

    PARAMS = dict(reportercode=reporterCode, period=period, publishedDateFrom=publishedDateFrom, publishedDateTo=publishedDateTo)
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
                        if (df.timestamp[i] is None):
                            timestamp = '1900-01-01'
                        else:
                            timestamp = df.timestamp[i][:10]  
                        fileName = "COMTRADE-" + prefixFile + "-" + df.typeCode[i] + df.freqCode[i] + str(df.reporterCode[i]).zfill(
                            3) + str(df.period[i]) + df.classificationCode[i] + "[" + timestamp + "].gz"
                    else:
                        if (df.publicationDate[i] is None):
                            publicationDate = '1900-01-01'
                        else:
                            publicationDate = df.publicationDate[i][:10]                          
                        fileName = "COMTRADE-" + prefixFile + "-" + df.typeCode[i] + df.freqCode[i] + str(
                            df.reporterCode[i]).zfill(
                            3) + str(df.period[i]) + df.classificationCode[i] + "[" + publicationDate + "].gz"
                    download_path = os.path.join(directory, fileName)
                    with open(download_path, "wb") as text:
                        for chunk in r.iter_content(chunk_size=1024):
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

def bulkDownloadFinalFile(subscription_key, directory, typeCode, freqCode, clCode, period, reporterCode, decompress, publishedDateFrom=None, publishedDateTo=None):
    bulkDownloadFile(subscription_key, directory, 'FINAL', typeCode, freqCode, clCode, period,
                             reporterCode, decompress, publishedDateFrom, publishedDateTo)

def bulkDownloadTarifflineFile(subscription_key, directory, typeCode, freqCode, clCode, period, reporterCode, decompress, publishedDateFrom=None, publishedDateTo=None):
    bulkDownloadFile(subscription_key, directory, 'TARIFFLINE', typeCode, freqCode, clCode, period,
                             reporterCode, decompress, publishedDateFrom, publishedDateTo)

def bulkDownloadFinalFileDateRange(subscription_key, directory, typeCode, freqCode, clCode, period, reporterCode, decompress, publishedDateFrom, publishedDateTo):
    bulkDownloadFile(subscription_key, directory, 'FINAL', typeCode, freqCode, clCode, period,
                             reporterCode, decompress, publishedDateFrom, publishedDateTo)

def bulkDownloadTarifflineFileDateRange(subscription_key, directory, typeCode, freqCode, clCode, period, reporterCode, decompress, publishedDateFrom, publishedDateTo):
    bulkDownloadFile(subscription_key, directory, 'TARIFFLINE', typeCode, freqCode, clCode, period,
                             reporterCode, decompress, publishedDateFrom, publishedDateTo)
