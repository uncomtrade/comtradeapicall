import os
import shutil
import gzip
from pandas import json_normalize
import urllib3
import json
from datetime import datetime


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


def bulkDownloadFinalFileDateRange(subscription_key, directory, typeCode, freqCode, clCode, period=None, reporterCode=None, decompress=False, publishedDateFrom=None, publishedDateTo=None, proxy_url=None):
    bulkDownloadFile(subscription_key, directory, 'FINAL', typeCode, freqCode, clCode, period,
                     reporterCode, decompress, publishedDateFrom, publishedDateTo, proxy_url)


def bulkDownloadFinalClassicFileDateRange(subscription_key, directory, typeCode, freqCode, clCode, period=None, reporterCode=None, decompress=False, publishedDateFrom=None, publishedDateTo=None, proxy_url=None):
    bulkDownloadFile(subscription_key, directory, 'FINALCLASSIC', typeCode, freqCode, clCode, period,
                     reporterCode, decompress, publishedDateFrom, publishedDateTo, proxy_url)


def bulkDownloadTarifflineFileDateRange(subscription_key, directory, typeCode, freqCode, clCode, period=None, reporterCode=None, decompress=False, publishedDateFrom=None, publishedDateTo=None, proxy_url=None):
    bulkDownloadFile(subscription_key, directory, 'TARIFFLINE', typeCode, freqCode, clCode, period,
                     reporterCode, decompress, publishedDateFrom, publishedDateTo, proxy_url)


def bulkDownloadAndCombineFileDateRange(subscription_key, directory, tradeDataType, typeCode, freqCode, clCode, period, reporterCode, decompress=False, publishedDateFrom=None, publishedDateTo=None, proxy_url=None):

    if tradeDataType == 'TARIFFLINE':
        bulkDownloadTarifflineFileDateRange(subscription_key, directory, typeCode=typeCode, freqCode=freqCode, clCode=clCode, period=period,
                                            reporterCode=reporterCode, decompress=False, publishedDateFrom=publishedDateFrom, publishedDateTo=publishedDateTo, proxy_url=proxy_url)
    elif tradeDataType == "FINALCLASSIC":
        bulkDownloadFinalClassicFileDateRange(subscription_key, directory, typeCode=typeCode, freqCode=freqCode, clCode=clCode, period=period,
                                              reporterCode=reporterCode, decompress=False, publishedDateFrom=publishedDateFrom, publishedDateTo=publishedDateTo, proxy_url=proxy_url)
    else:
        bulkDownloadFinalFileDateRange(subscription_key, directory, typeCode=typeCode, freqCode=freqCode, clCode=clCode, period=period,
                                       reporterCode=reporterCode, decompress=False, publishedDateFrom=publishedDateFrom, publishedDateTo=publishedDateTo, proxy_url=proxy_url)

    # Generate timestamp for filenames
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")  # e.g., 20251203_114500

    output_file_name = 'COMBINED-COMTRADE-' + tradeDataType + \
        "-" + typeCode + freqCode+clCode + "-" + \
        (str(reporterCode).zfill(3) if reporterCode is not None else "ALL") + "-" +\
        (str(period) if period is not None else "ALL")

    input_folder = directory
    txt_output = os.path.join(
        input_folder, output_file_name + f'-[{timestamp}].txt')
    gz_output = os.path.join(
        input_folder, output_file_name + f'-[{timestamp}].gz')

    # Get all .gz files in the folder that start with "COMTRADE-"
    gz_files = sorted([
        os.path.join(input_folder, f)
        for f in os.listdir(input_folder)
        if f.startswith('COMTRADE-') and f.endswith('.gz')
    ])

    # ✅ Pre-check: Ensure files exist before doing any work
    if not gz_files:
        print("❌ No matching .gz files found. Nothing to process.")
    else:
        try:
            # Step 1: Write combined content to a .txt file
            with open(txt_output, 'w', encoding='utf-8') as txt_out:
                header_written = False
                for file in gz_files:
                    print(f"Processing: {file}")
                    with gzip.open(file, 'rt', encoding='utf-8') as in_f:
                        for i, line in enumerate(in_f):
                            if i == 0:
                                if not header_written:
                                    # Write header from first file
                                    txt_out.write(line)
                                    header_written = True
                                # Skip header for subsequent files
                            else:
                                txt_out.write(line)

            # Step 2: Compress the combined .txt file into .gz
            if (not decompress):
                with open(txt_output, 'rb') as f_in:
                    with gzip.GzipFile(filename=txt_output, mode='wb', fileobj=open(gz_output, 'wb')) as f_out:
                        shutil.copyfileobj(f_in, f_out)

            # Step 3: Delete original .gz files
            for file in gz_files:
                os.remove(file)
                print(f"Deleted: {file}")
            if (not decompress):
                os.remove(txt_output)
                print(f"Deleted: {file}")

            print(
                f"✅ Combined {len(gz_files)} files into {gz_output}.")

        except Exception as e:
            print(f"❌ An error occurred: {e}")


def bulkDownloadAndCombineTarifflineFile(subscription_key, directory, typeCode, freqCode, clCode, period, reporterCode, decompress=False, publishedDateFrom=None, publishedDateTo=None, proxy_url=None):
    bulkDownloadAndCombineFileDateRange(subscription_key, directory, 'TARIFFLINE', typeCode, freqCode,
                                        clCode, period, reporterCode, decompress, publishedDateFrom, publishedDateTo, proxy_url)


def bulkDownloadAndCombineFinalFile(subscription_key, directory, typeCode, freqCode, clCode, period, reporterCode, decompress=False, publishedDateFrom=None, publishedDateTo=None, proxy_url=None):
    bulkDownloadAndCombineFileDateRange(subscription_key, directory, 'FINAL', typeCode, freqCode,
                                        clCode, period, reporterCode, decompress, publishedDateFrom, publishedDateTo, proxy_url)


def bulkDownloadAndCombineFinalClassicFile(subscription_key, directory, typeCode, freqCode, clCode, period, reporterCode, decompress=False, publishedDateFrom=None, publishedDateTo=None, proxy_url=None):
    bulkDownloadAndCombineFileDateRange(subscription_key, directory, 'FINALCLASSIC', typeCode, freqCode,
                                        clCode, period, reporterCode, decompress, publishedDateFrom, publishedDateTo, proxy_url)
