import pandas as pd
import json
from pandas import json_normalize
import urllib3


def getMetadata(subscription_key, typeCode, freqCode, clCode, period, reporterCode, showHistory, proxy_url=None):
    if (subscription_key is None):
        endpoint = "public"
    else:
        endpoint = "data"

    baseURL = 'https://comtradeapi.un.org/' + endpoint + \
        '/v1/getMetadata/' + typeCode + '/' + freqCode + '/' + clCode
    PARAMS = dict(reporterCode=reporterCode, period=period)
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
            jsonResult = json.loads(resp.data.decode('utf-8-sig'))
            df = json_normalize(jsonResult['data'])
            if df.empty:
                return pd.DataFrame()
            if 'notes' not in df.columns:
                raise ValueError('Metadata response is missing notes')
            FIELDS = ['notes']
            dt = df[FIELDS]
            dt = dt.explode('notes')
            df_final = (
                pd.DataFrame(dt["notes"]
                             .apply(pd.Series))
            )
            if df_final.empty:
                return pd.DataFrame()
            if not {'datasetCode', 'publicationDate'}.issubset(df_final.columns):
                raise ValueError('Metadata notes missing datasetCode or publicationDate')
            dt_final_latest = df_final[['datasetCode', 'publicationDate']].groupby(
                'datasetCode', as_index=False)['publicationDate'].max()
            dt_final_latest.loc[:, 'isLatestPublication'] = True
            df_final_merge = df_final.merge(
                dt_final_latest, on=['datasetCode', 'publicationDate'], how='left', validate='many_to_one')
            if (showHistory):
                return df_final_merge
            else:
                return df_final_merge.loc[df_final_merge['isLatestPublication'].eq(True)].copy()
    except urllib3.exceptions.HTTPError as err:
        raise RuntimeError('Comtrade transport request failed') from err


def _getMetadata(typeCode, freqCode, clCode, period, reporterCode, showHistory):
    return getMetadata(None, typeCode, freqCode, clCode, period, reporterCode, showHistory)


def listReference(category=None, proxy_url=None):
    baseURL = 'https://comtradeapi.un.org/files/v1/app/reference/ListofReferences.json'
    try:
        if proxy_url:
            http = urllib3.ProxyManager(proxy_url=proxy_url)
        else:
            http = urllib3.PoolManager()
        resp = http.request("GET", baseURL, timeout=120)
        if resp.status != 200:
            raise RuntimeError(f"HTTP {resp.status}: {resp.data.decode('utf-8', errors='replace')}")
        else:
            jsonResult = json.loads(resp.data.decode('utf-8-sig'))
            df = json_normalize(jsonResult['results'])
            if category is not None:
                return df.loc[df['category'].eq(category)].copy()
            else:
                return df
    except urllib3.exceptions.HTTPError as err:
        raise RuntimeError('Comtrade transport request failed') from err


def getReference(category, proxy_url=None):
    try:
        baseURL = listReference(category, proxy_url=proxy_url).iloc[0]['fileuri']
    except (AttributeError, IndexError, KeyError, TypeError) as err:
        raise ValueError(f'No reference URI for category {category!r}') from err
    if baseURL != '':
        try:
            if proxy_url:
                http = urllib3.ProxyManager(proxy_url=proxy_url)
            else:
                http = urllib3.PoolManager()
            resp = http.request("GET", baseURL, timeout=120)
            if resp.status != 200:
                raise RuntimeError(f"HTTP {resp.status}: {resp.data.decode('utf-8', errors='replace')}")
            else:
                jsonResult = json.loads(resp.data.decode('utf-8-sig'))
                # Results contain the required data
                df = json_normalize(jsonResult['results'])
                return df
        except urllib3.exceptions.HTTPError as err:
            raise RuntimeError('Comtrade transport request failed') from err


def convertCountryIso3ToCode(countryIsoCode, proxy_url=None):
    baseURL = 'https://comtradeapi.un.org/files/v1/app/reference/country_area_code_iso.json'
    if proxy_url:
        http = urllib3.ProxyManager(proxy_url=proxy_url)
    else:
        http = urllib3.PoolManager()
    resp = http.request("GET", baseURL, timeout=120)
    if resp.status != 200:
        raise RuntimeError(f'Country reference HTTP {resp.status}')
    df = json_normalize(json.loads(resp.data.decode('utf-8-sig'))['results'])
    df['country_area_code'] = df['country_area_code'].astype(str)
    delim = ','
    iso_string = countryIsoCode
    iso_list = [v.strip().upper() for v in iso_string.split(delim) if v.strip()]
    code_list = df[df['iso3'].astype(str).str.strip().str.upper().isin(iso_list)]['country_area_code'].tolist()
    code_string = delim.join(code_list)
    return code_string
