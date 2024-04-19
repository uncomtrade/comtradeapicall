import os
import shutil
import gzip
from pandas import json_normalize
import urllib3
import json


def bulkDownloadFile(subscription_key, directory, tradeDataType, typeCode, freqCode, clCode, period, reporterCode,
                     decompress, publishedDateFrom=None, publishedDateTo=None, proxy_url=None):

    if tradeDataType == 'TARIFFLINE':
        baseURLDataAvailability = 'https://comtradeapi.un.org/bulk/v1/getTariffline/' + \
            typeCode + '/' + freqCode + '/' + clCode
        prefixFile = 'TARIFFLINE'
    elif tradeDataType == "FINALCLASSIC":
        baseURLDataAvailability = 'https://comtradeapi.un.org/bulk/v1/getClassic/' + \
            typeCode + '/' + freqCode + '/' + clCode
        prefixFile = 'FINALCLASSIC'
    else:
        baseURLDataAvailability = 'https://comtradeapi.un.org/bulk/v1/get/' + \
            typeCode + '/' + freqCode + '/' + clCode
        prefixFile = 'FINAL'

    PARAMS = dict(reportercode=reporterCode, period=period,
                  publishedDateFrom=publishedDateFrom, publishedDateTo=publishedDateTo)
    # add key
    PARAMS["subscription-key"] = subscription_key
    fields = dict(filter(lambda item: item[1] is not None, PARAMS.items()))
    if proxy_url:
        http = urllib3.ProxyManager(proxy_url=proxy_url)
    else:
        http = urllib3.PoolManager()
    try:
        resp = http.request("GET", baseURLDataAvailability,
                            fields=fields, timeout=120)
        if resp.status != 200:
            print(resp.data.decode('utf-8'))
        else:
            jsonResult = json.loads(resp.data)
            if jsonResult['count'] == 0:
                print('No data available based on the selection criteria')
            else:
                # Results contain the required data
                df = json_normalize(jsonResult['data'])
                totalFiles = df[df.columns[0]].count()
                i = 0
                while i < totalFiles:
                    # prepare file name
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
                    # download file
                    file_url = df.fileUrl[i]
                    PARAMS = dict()
                    PARAMS["subscription-key"] = subscription_key
                    fieldsFILE = dict(
                        filter(lambda item: item[1] is not None, PARAMS.items()))
                    if proxy_url:
                        httpFILE = urllib3.ProxyManager(proxy_url=proxy_url)
                    else:
                        httpFILE = urllib3.PoolManager()
                    with open(download_path, 'wb') as out:
                        r = httpFILE.request(
                            'GET', file_url, fields=fieldsFILE, preload_content=False)
                        shutil.copyfileobj(r, out)
                    r.release_conn()
                    print(fileName.replace(".gz", "") + ' downloaded')
                    download_path_gunzip = download_path.replace(".gz", ".txt")
                    if decompress is True:
                        with gzip.open(download_path, "rb") as f_in:
                            with open(download_path_gunzip, 'wb') as f_out:
                                shutil.copyfileobj(f_in, f_out)
                        os.remove(download_path)
                    i = i + 1
                print('Total of ' + str(i) + ' file(s) downloaded')
    except urllib3.exceptions.RequestError as err:
        print(f'Request error: {err}')


def bulkDownloadFinalFile(subscription_key, directory, typeCode, freqCode, clCode, period=None, reporterCode=None, decompress=False, publishedDateFrom=None, publishedDateTo=None):
    bulkDownloadFile(subscription_key, directory, 'FINAL', typeCode, freqCode, clCode, period,
                     reporterCode, decompress, publishedDateFrom, publishedDateTo)


def bulkDownloadFinalClassicFile(subscription_key, directory, typeCode, freqCode, clCode, period=None, reporterCode=None, decompress=False, publishedDateFrom=None, publishedDateTo=None):
    bulkDownloadFile(subscription_key, directory, 'FINALCLASSIC', typeCode, freqCode, clCode, period,
                     reporterCode, decompress, publishedDateFrom, publishedDateTo)


def bulkDownloadTarifflineFile(subscription_key, directory, typeCode, freqCode, clCode, period=None, reporterCode=None, decompress=False, publishedDateFrom=None, publishedDateTo=None):
    bulkDownloadFile(subscription_key, directory, 'TARIFFLINE', typeCode, freqCode, clCode, period,
                     reporterCode, decompress, publishedDateFrom, publishedDateTo)


def bulkDownloadFinalFileDateRange(subscription_key, directory, typeCode, freqCode, clCode, period=None, reporterCode=None, decompress=False, publishedDateFrom=None, publishedDateTo=None):
    bulkDownloadFile(subscription_key, directory, 'FINAL', typeCode, freqCode, clCode, period,
                     reporterCode, decompress, publishedDateFrom, publishedDateTo)


def bulkDownloadFinalClassicFileDateRange(subscription_key, directory, typeCode, freqCode, clCode, period=None, reporterCode=None, decompress=False, publishedDateFrom=None, publishedDateTo=None):
    bulkDownloadFile(subscription_key, directory, 'FINALCLASSIC', typeCode, freqCode, clCode, period,
                     reporterCode, decompress, publishedDateFrom, publishedDateTo)


def bulkDownloadTarifflineFileDateRange(subscription_key, directory, typeCode, freqCode, clCode, period=None, reporterCode=None, decompress=False, publishedDateFrom=None, publishedDateTo=None):
    bulkDownloadFile(subscription_key, directory, 'TARIFFLINE', typeCode, freqCode, clCode, period,
                     reporterCode, decompress, publishedDateFrom, publishedDateTo)
