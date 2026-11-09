import os
import shutil
import gzip
from pandas import json_normalize
import urllib3
import json
from datetime import datetime


def bulkDownloadFile(
    subscription_key,
    directory,
    tradeDataType,
    typeCode,
    freqCode,
    clCode,
    period,
    reporterCode,
    decompress,
    publishedDateFrom=None,
    publishedDateTo=None,
    proxy_url=None,
):
    if tradeDataType == "TRADEMATRIX":
        _validate_trade_matrix_selection(period, reporterCode)
        clCode = "TM"
    elif any(
        value is not None and "allcombined" in str(value).lower().split(",")
        for value in (period, reporterCode)
    ):
        raise ValueError("allcombined is supported only for Trade Matrix")
    os.makedirs(directory, exist_ok=True)

    if tradeDataType == "TARIFFLINE":
        baseURLDataAvailability = (
            "https://comtradeapi.un.org/bulk/v1/getTariffline/"
            + typeCode
            + "/"
            + freqCode
            + "/"
            + clCode
        )
        prefixFile = "TARIFFLINE"
    elif tradeDataType == "FINALCLASSIC":
        baseURLDataAvailability = (
            "https://comtradeapi.un.org/bulk/v1/getClassic/"
            + typeCode
            + "/"
            + freqCode
            + "/"
            + clCode
        )
        prefixFile = "FINALCLASSIC"
    else:
        baseURLDataAvailability = (
            "https://comtradeapi.un.org/bulk/v1/get/"
            + typeCode
            + "/"
            + freqCode
            + "/"
            + clCode
        )
        prefixFile = "TRADEMATRIX" if tradeDataType == "TRADEMATRIX" else "FINAL"

    PARAMS = dict(
        reportercode=reporterCode,
        period=period,
        publishedDateFrom=publishedDateFrom,
        publishedDateTo=publishedDateTo,
    )
    # add key
    PARAMS["subscription-key"] = subscription_key
    fields = dict(filter(lambda item: item[1] is not None, PARAMS.items()))
    if proxy_url:
        http = urllib3.ProxyManager(proxy_url=proxy_url)
    else:
        http = urllib3.PoolManager()
    try:
        resp = http.request("GET", baseURLDataAvailability, fields=fields, timeout=120)
        if resp.status != 200:
            raise RuntimeError(
                f"HTTP {resp.status}: {resp.data.decode('utf-8', errors='replace')}"
            )
        else:
            jsonResult = json.loads(resp.data)
            if jsonResult["count"] == 0:
                print("No data available based on the selection criteria")
            else:
                # Results contain the required data
                df = json_normalize(jsonResult["data"])
                totalFiles = df[df.columns[0]].count()
                i = 0
                while i < totalFiles:
                    # prepare file name
                    if tradeDataType == "TARIFFLINE":
                        if df.timestamp[i] is None:
                            timestamp = "1900-01-01"
                        else:
                            timestamp = df.timestamp[i][:10]
                        fileName = (
                            "COMTRADE-"
                            + prefixFile
                            + "-"
                            + df.typeCode[i]
                            + df.freqCode[i]
                            + (
                                _trade_matrix_filename_token(df.reporterCode[i], 3)
                                if tradeDataType == "TRADEMATRIX"
                                else str(df.reporterCode[i]).zfill(3)
                            )
                            + (
                                _trade_matrix_filename_token(df.period[i])
                                if tradeDataType == "TRADEMATRIX"
                                else str(df.period[i])
                            )
                            + df.classificationCode[i]
                            + "["
                            + timestamp
                            + "].gz"
                        )
                    else:
                        if (
                            "publicationDate" not in df.columns
                            or df.publicationDate[i] is None
                        ):
                            publicationDate = "1900-01-01"
                        else:
                            publicationDate = df.publicationDate[i][:10]
                        fileName = (
                            "COMTRADE-"
                            + prefixFile
                            + "-"
                            + df.typeCode[i]
                            + df.freqCode[i]
                            + (
                                _trade_matrix_filename_token(df.reporterCode[i], 3)
                                if tradeDataType == "TRADEMATRIX"
                                else str(df.reporterCode[i]).zfill(3)
                            )
                            + (
                                _trade_matrix_filename_token(df.period[i])
                                if tradeDataType == "TRADEMATRIX"
                                else str(df.period[i])
                            )
                            + df.classificationCode[i]
                            + "["
                            + publicationDate
                            + "].gz"
                        )
                    download_path = os.path.join(directory, os.path.basename(fileName))
                    # download file
                    file_url = df.fileUrl[i]
                    PARAMS = dict()
                    PARAMS["subscription-key"] = subscription_key
                    fieldsFILE = dict(
                        filter(lambda item: item[1] is not None, PARAMS.items())
                    )
                    if proxy_url:
                        httpFILE = urllib3.ProxyManager(proxy_url=proxy_url)
                    else:
                        httpFILE = urllib3.PoolManager()
                    _save_download(httpFILE, file_url, download_path, fieldsFILE)
                    print(fileName.replace(".gz", "") + " downloaded")
                    download_path_gunzip = download_path.replace(".gz", ".txt")
                    if decompress is True:
                        with gzip.open(download_path, "rb") as f_in:
                            with open(download_path_gunzip, "wb") as f_out:
                                shutil.copyfileobj(f_in, f_out)
                        os.remove(download_path)
                    i = i + 1
                print("Total of " + str(i) + " file(s) downloaded")
    except urllib3.exceptions.HTTPError as err:
        raise RuntimeError("Comtrade transport request failed") from err


def bulkDownloadFinalFile(
    subscription_key,
    directory,
    typeCode,
    freqCode,
    clCode,
    period=None,
    reporterCode=None,
    decompress=False,
    publishedDateFrom=None,
    publishedDateTo=None,
    proxy_url=None,
):
    bulkDownloadFile(
        subscription_key,
        directory,
        "FINAL",
        typeCode,
        freqCode,
        clCode,
        period,
        reporterCode,
        decompress,
        publishedDateFrom,
        publishedDateTo,
        proxy_url,
    )


def bulkDownloadFinalClassicFile(
    subscription_key,
    directory,
    typeCode,
    freqCode,
    clCode,
    period=None,
    reporterCode=None,
    decompress=False,
    publishedDateFrom=None,
    publishedDateTo=None,
    proxy_url=None,
):
    bulkDownloadFile(
        subscription_key,
        directory,
        "FINALCLASSIC",
        typeCode,
        freqCode,
        clCode,
        period,
        reporterCode,
        decompress,
        publishedDateFrom,
        publishedDateTo,
        proxy_url,
    )


def bulkDownloadTarifflineFile(
    subscription_key,
    directory,
    typeCode,
    freqCode,
    clCode,
    period=None,
    reporterCode=None,
    decompress=False,
    publishedDateFrom=None,
    publishedDateTo=None,
    proxy_url=None,
):
    bulkDownloadFile(
        subscription_key,
        directory,
        "TARIFFLINE",
        typeCode,
        freqCode,
        clCode,
        period,
        reporterCode,
        decompress,
        publishedDateFrom,
        publishedDateTo,
        proxy_url,
    )


def bulkDownloadFinalFileDateRange(
    subscription_key,
    directory,
    typeCode,
    freqCode,
    clCode,
    period=None,
    reporterCode=None,
    decompress=False,
    publishedDateFrom=None,
    publishedDateTo=None,
    proxy_url=None,
):
    bulkDownloadFile(
        subscription_key,
        directory,
        "FINAL",
        typeCode,
        freqCode,
        clCode,
        period,
        reporterCode,
        decompress,
        publishedDateFrom,
        publishedDateTo,
        proxy_url,
    )


def bulkDownloadFinalClassicFileDateRange(
    subscription_key,
    directory,
    typeCode,
    freqCode,
    clCode,
    period=None,
    reporterCode=None,
    decompress=False,
    publishedDateFrom=None,
    publishedDateTo=None,
    proxy_url=None,
):
    bulkDownloadFile(
        subscription_key,
        directory,
        "FINALCLASSIC",
        typeCode,
        freqCode,
        clCode,
        period,
        reporterCode,
        decompress,
        publishedDateFrom,
        publishedDateTo,
        proxy_url,
    )


def bulkDownloadTarifflineFileDateRange(
    subscription_key,
    directory,
    typeCode,
    freqCode,
    clCode,
    period=None,
    reporterCode=None,
    decompress=False,
    publishedDateFrom=None,
    publishedDateTo=None,
    proxy_url=None,
):
    bulkDownloadFile(
        subscription_key,
        directory,
        "TARIFFLINE",
        typeCode,
        freqCode,
        clCode,
        period,
        reporterCode,
        decompress,
        publishedDateFrom,
        publishedDateTo,
        proxy_url,
    )


def bulkDownloadAndCombineFileDateRange(
    subscription_key,
    directory,
    tradeDataType,
    typeCode,
    freqCode,
    clCode,
    period,
    reporterCode,
    decompress=False,
    publishedDateFrom=None,
    publishedDateTo=None,
    proxy_url=None,
):
    import tempfile

    os.makedirs(directory, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d")
    output_name = (
        "COMBINED-COMTRADE-"
        + tradeDataType
        + "-"
        + typeCode
        + freqCode
        + clCode
        + "-"
        + (str(reporterCode).zfill(3) if reporterCode is not None else "ALL")
        + "-"
        + (str(period) if period is not None else "ALL")
        + "-["
        + timestamp
        + "]"
    )
    destination = os.path.join(
        directory, output_name + (".txt" if decompress else ".gz")
    )
    # No pre-existing user files are read, overwritten, or deleted as source files.
    with tempfile.TemporaryDirectory(dir=directory) as temporary:
        bulkDownloadFile(
            subscription_key,
            temporary,
            tradeDataType,
            typeCode,
            freqCode,
            clCode,
            period,
            reporterCode,
            False,
            publishedDateFrom,
            publishedDateTo,
            proxy_url,
        )
        sources = sorted(
            os.path.join(temporary, name)
            for name in os.listdir(temporary)
            if name.startswith("COMTRADE-") and name.endswith(".gz")
        )
        if not sources:
            print("No matching files downloaded. Nothing to combine.")
            return
        combined = os.path.join(temporary, "combined.txt")
        header = None
        with open(combined, "w", encoding="utf-8", newline="") as output:
            for source in sources:
                with gzip.open(
                    source, "rt", encoding="utf-8-sig", newline=""
                ) as incoming:
                    current_header = incoming.readline()
                    if not current_header:
                        raise ValueError(
                            f"Empty bulk source: {os.path.basename(source)}"
                        )
                    if header is None:
                        header = current_header.rstrip("\r\n")
                        output.write(header + "\n")
                    elif current_header.rstrip("\r\n") != header:
                        raise ValueError(
                            "Bulk files have inconsistent headers; no output published"
                        )
                    for line in incoming:
                        output.write(line.rstrip("\r\n") + "\n")
        if decompress:
            os.replace(combined, destination)
        else:
            compressed = os.path.join(temporary, "combined.gz")
            # Store the final text filename in the gzip header, not the
            # temporary name "combined". Do not include a directory path.
            inner_filename = os.path.splitext(os.path.basename(destination))[0] + ".txt"
            with open(combined, "rb") as incoming, open(compressed, "wb") as raw_output:
                with gzip.GzipFile(
                    filename=inner_filename, mode="wb", fileobj=raw_output
                ) as output:
                    shutil.copyfileobj(incoming, output)
            os.replace(compressed, destination)
        print(f"Combined {len(sources)} files into {destination}")


def bulkDownloadAndCombineTarifflineFile(
    subscription_key,
    directory,
    typeCode,
    freqCode,
    clCode,
    period,
    reporterCode,
    decompress=False,
    publishedDateFrom=None,
    publishedDateTo=None,
    proxy_url=None,
):
    bulkDownloadAndCombineFileDateRange(
        subscription_key,
        directory,
        "TARIFFLINE",
        typeCode,
        freqCode,
        clCode,
        period,
        reporterCode,
        decompress,
        publishedDateFrom,
        publishedDateTo,
        proxy_url,
    )


def bulkDownloadAndCombineFinalFile(
    subscription_key,
    directory,
    typeCode,
    freqCode,
    clCode,
    period,
    reporterCode,
    decompress=False,
    publishedDateFrom=None,
    publishedDateTo=None,
    proxy_url=None,
):
    bulkDownloadAndCombineFileDateRange(
        subscription_key,
        directory,
        "FINAL",
        typeCode,
        freqCode,
        clCode,
        period,
        reporterCode,
        decompress,
        publishedDateFrom,
        publishedDateTo,
        proxy_url,
    )


def bulkDownloadAndCombineFinalClassicFile(
    subscription_key,
    directory,
    typeCode,
    freqCode,
    clCode,
    period,
    reporterCode,
    decompress=False,
    publishedDateFrom=None,
    publishedDateTo=None,
    proxy_url=None,
):
    bulkDownloadAndCombineFileDateRange(
        subscription_key,
        directory,
        "FINALCLASSIC",
        typeCode,
        freqCode,
        clCode,
        period,
        reporterCode,
        decompress,
        publishedDateFrom,
        publishedDateTo,
        proxy_url,
    )


def _save_download(http, url, destination, fields=None):
    """Validate response and atomically publish a completed download."""
    import tempfile

    directory = os.path.dirname(os.path.abspath(destination))
    os.makedirs(directory, exist_ok=True)
    response = http.request(
        "GET", url, fields=fields, preload_content=False, timeout=120
    )
    temporary = None
    try:
        if response.status != 200:
            raise RuntimeError(f"Download HTTP {response.status}")
        with tempfile.NamedTemporaryFile(dir=directory, delete=False) as output:
            temporary = output.name
            shutil.copyfileobj(response, output)
        os.replace(temporary, destination)
        temporary = None
    finally:
        response.release_conn()
        if temporary and os.path.exists(temporary):
            os.remove(temporary)


def _validate_trade_matrix_selection(period, reporterCode):
    """allcombined is exclusive to Trade Matrix and cannot select both axes."""

    def has_combined(value):
        return value is not None and any(
            token.strip().lower() == "allcombined" for token in str(value).split(",")
        )

    for name, value in [("period", period), ("reporterCode", reporterCode)]:
        if has_combined(value) and str(value).strip().lower() != "allcombined":
            raise ValueError(f"{name}: allcombined must be used alone, not in a list")
    if has_combined(period) and has_combined(reporterCode):
        raise ValueError(
            "reporterCode=allcombined and period=allcombined cannot be used together"
        )


def bulkDownloadTradeMatrixFile(
    subscription_key,
    directory,
    typeCode,
    freqCode,
    period=None,
    reporterCode=None,
    decompress=False,
    publishedDateFrom=None,
    publishedDateTo=None,
    proxy_url=None,
):
    """Download Trade Matrix files with classification fixed to TM.

    reporterCode or period may be 'allcombined', but never both together.
    reporterCode retains the existing library parameter spelling.
    """
    _validate_trade_matrix_selection(period, reporterCode)
    bulkDownloadFile(
        subscription_key,
        directory,
        "TRADEMATRIX",
        typeCode,
        freqCode,
        "TM",
        period,
        reporterCode,
        decompress,
        publishedDateFrom,
        publishedDateTo,
        proxy_url,
    )


def bulkDownloadTradeMatrixFileDateRange(
    subscription_key,
    directory,
    typeCode,
    freqCode,
    period=None,
    reporterCode=None,
    decompress=False,
    publishedDateFrom=None,
    publishedDateTo=None,
    proxy_url=None,
):
    """Download Trade Matrix files with classification fixed to TM.

    reporterCode or period may be 'allcombined', but never both together.
    reporterCode retains the existing library parameter spelling.
    """
    _validate_trade_matrix_selection(period, reporterCode)
    bulkDownloadFile(
        subscription_key,
        directory,
        "TRADEMATRIX",
        typeCode,
        freqCode,
        "TM",
        period,
        reporterCode,
        decompress,
        publishedDateFrom,
        publishedDateTo,
        proxy_url,
    )


def bulkDownloadAndCombineTradeMatrixFile(
    subscription_key,
    directory,
    typeCode,
    freqCode,
    period=None,
    reporterCode=None,
    decompress=False,
    publishedDateFrom=None,
    publishedDateTo=None,
    proxy_url=None,
):
    """Download Trade Matrix files with classification fixed to TM.

    reporterCode or period may be 'allcombined', but never both together.
    reporterCode retains the existing library parameter spelling.
    """
    _validate_trade_matrix_selection(period, reporterCode)
    bulkDownloadAndCombineFileDateRange(
        subscription_key,
        directory,
        "TRADEMATRIX",
        typeCode,
        freqCode,
        "TM",
        period,
        reporterCode,
        decompress,
        publishedDateFrom,
        publishedDateTo,
        proxy_url,
    )


def _trade_matrix_filename_token(value, width=0):
    """Use ALL for missing/combined metadata, without changing API filters."""
    from pandas import isna

    if value is None or isna(value):
        return "ALL"
    text = str(value).strip()
    if text.lower() in ("allcombined", "none", "nan", "null", "<na>", ""):
        return "ALL"
    # Nullable numeric columns may turn 600 into 600.0.
    if isinstance(value, (int, float)) and float(value).is_integer():
        text = str(int(value))
    return text.zfill(width) if width else text
