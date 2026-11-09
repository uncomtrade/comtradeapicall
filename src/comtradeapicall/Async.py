import time as t
from pandas import json_normalize
import os
from urllib.parse import urlparse
import json
import urllib3
import shutil


def submitAsyncDataRequest(subscription_key, endPoint, typeCode, freqCode, clCode, period, reporterCode, cmdCode,
                           flowCode, partnerCode, partner2Code, customsCode, motCode, aggregateBy, breakdownMode, proxy_url=None):
    if endPoint == 'TARIFFLINE':
        baseURL = 'https://comtradeapi.un.org/async/v1/getTariffline/' + \
            typeCode + '/' + freqCode + '/' + clCode
    else:
        baseURL = 'https://comtradeapi.un.org/async/v1/get/' + \
            typeCode + '/' + freqCode + '/' + clCode

    PARAMS = dict(reportercode=reporterCode, flowCode=flowCode,
                  period=period, cmdCode=cmdCode, partnerCode=partnerCode, partner2Code=partner2Code,
                  motCode=motCode, customsCode=customsCode, aggregateBy=aggregateBy, breakdownMode=breakdownMode)
    PARAMS["subscription-key"] = subscription_key
    fields = dict(filter(lambda item: item[1] is not None, PARAMS.items()))
    if proxy_url:
        http = urllib3.ProxyManager(proxy_url=proxy_url)
    else:
        http = urllib3.PoolManager()
    try:
        resp = http.request("GET", baseURL, fields=fields, timeout=120)
        if resp.status != 202:
            raise RuntimeError(f"HTTP {resp.status}: {resp.data.decode('utf-8', errors='replace')}")
        else:
            jsonResult = json.loads(resp.data)
            print('Return message:', jsonResult['message'])
            return jsonResult
    except urllib3.exceptions.HTTPError as err:
        raise RuntimeError('Comtrade transport request failed') from err


def submitAsyncFinalDataRequest(subscription_key, typeCode, freqCode, clCode, period, reporterCode, cmdCode, flowCode,
                                partnerCode,
                                partner2Code, customsCode, motCode,  aggregateBy=None,
                                breakdownMode=None, proxy_url=None):
    return submitAsyncDataRequest(subscription_key, 'FINAL', typeCode, freqCode, clCode, period, reporterCode,
                                  cmdCode, flowCode,
                                  partnerCode,
                                  partner2Code, customsCode, motCode, aggregateBy, breakdownMode, proxy_url=proxy_url)


def submitAsyncTarifflineDataRequest(subscription_key, typeCode, freqCode, clCode, period, reporterCode, cmdCode,
                                     flowCode, partnerCode, partner2Code, customsCode, motCode, proxy_url=None):
    return submitAsyncDataRequest(subscription_key, 'TARIFFLINE', typeCode, freqCode, clCode, period, reporterCode,
                                  cmdCode, flowCode,
                                  partnerCode,
                                  partner2Code, customsCode, motCode, aggregateBy=None, breakdownMode=None, proxy_url=proxy_url)


def checkAsyncDataRequest(subscription_key, batchId=None, proxy_url=None):
    baseURL = 'https://comtradeapi.un.org/async/v1/getDA/'
    PARAMS = dict(batchId=batchId)
    PARAMS["subscription-key"] = subscription_key
    fields = dict(filter(lambda item: item[1] is not None, PARAMS.items()))
    if proxy_url:
        http = urllib3.ProxyManager(proxy_url=proxy_url)
    else:
        http = urllib3.PoolManager()
    try:
        resp = http.request("GET", baseURL, fields=fields, timeout=120)
        if resp.status != 200:
            raise RuntimeError(f"HTTP {resp.status}: {resp.data.decode('utf-8', errors='replace')}")
        else:
            jsonResult = json.loads(resp.data)
            df = json_normalize(jsonResult['data'])
            return df
    except urllib3.exceptions.HTTPError as err:
        raise RuntimeError('Comtrade transport request failed') from err


def downloadAsyncFinalDataRequest(subscription_key, directory, typeCode, freqCode, clCode, period,
                                  reporterCode, cmdCode, flowCode, partnerCode, partner2Code, customsCode, motCode,
                                  aggregateBy=None, breakdownMode=None, proxy_url=None, poll_interval=15, max_wait_seconds=3600):
    result = submitAsyncFinalDataRequest(subscription_key, typeCode, freqCode, clCode, period, reporterCode, cmdCode, flowCode, partnerCode, partner2Code, customsCode, motCode, aggregateBy, breakdownMode, proxy_url=proxy_url)
    _download_async_result(subscription_key, directory, result, proxy_url, poll_interval, max_wait_seconds)


def downloadAsyncTarifflineDataRequest(subscription_key, directory, typeCode, freqCode, clCode, period,
                                       reporterCode, cmdCode, flowCode, partnerCode, partner2Code, customsCode, motCode, proxy_url=None, poll_interval=15, max_wait_seconds=3600):
    result = submitAsyncTarifflineDataRequest(subscription_key, typeCode, freqCode, clCode, period, reporterCode, cmdCode, flowCode, partnerCode, partner2Code, customsCode, motCode, proxy_url=proxy_url)
    _download_async_result(subscription_key, directory, result, proxy_url, poll_interval, max_wait_seconds)


def _download_async_result(subscription_key, directory, result, proxy_url, poll_interval, max_wait_seconds):
    if not isinstance(result, dict) or not result.get('requestId'):
        raise RuntimeError('The asynchronous request was not accepted')
    if poll_interval <= 0 or max_wait_seconds <= 0:
        raise ValueError('Polling interval and maximum wait must be positive')
    os.makedirs(directory, exist_ok=True)
    batch_id = result['requestId']
    deadline = t.monotonic() + max_wait_seconds
    while True:
        if t.monotonic() >= deadline:
            raise TimeoutError(f'Async request {batch_id} timed out')
        frame = checkAsyncDataRequest(subscription_key, batchId=batch_id, proxy_url=proxy_url)
        if frame is None or frame.empty or 'status' not in frame.columns:
            raise RuntimeError(f'Invalid status response for batch {batch_id}')
        status = str(frame.iloc[0]['status'])
        if status == 'Completed':
            break
        if status.lower() in {'error', 'failed', 'cancelled', 'canceled', 'expired'}:
            raise RuntimeError(f'Async request {batch_id} ended with {status}')
        t.sleep(min(poll_interval, max(0, deadline - t.monotonic())))
    if 'uri' not in frame.columns or not isinstance(frame.iloc[0]['uri'], str):
        raise RuntimeError('Completed request did not provide a download URI')
    url = frame.iloc[0]['uri']
    filename = os.path.basename(urlparse(url).path)
    if filename in {'', '.', '..'}:
        raise ValueError('Invalid download filename')
    http = urllib3.ProxyManager(proxy_url=proxy_url) if proxy_url else urllib3.PoolManager()
    _save_download(http, url, os.path.join(directory, filename))
    print(filename, 'downloaded successfully')


def _save_download(http, url, destination, fields=None):
    """Validate response and atomically publish a completed download."""
    import tempfile
    directory = os.path.dirname(os.path.abspath(destination))
    os.makedirs(directory, exist_ok=True)
    response = http.request('GET', url, fields=fields, preload_content=False, timeout=120)
    temporary = None
    try:
        if response.status != 200:
            raise RuntimeError(f'Download HTTP {response.status}')
        with tempfile.NamedTemporaryFile(dir=directory, delete=False) as output:
            temporary = output.name
            shutil.copyfileobj(response, output)
        os.replace(temporary, destination)
        temporary = None
    finally:
        response.release_conn()
        if temporary and os.path.exists(temporary):
            os.remove(temporary)
