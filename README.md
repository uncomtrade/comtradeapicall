# UN Comtrade API Python package

`comtradeapicall` simplifies retrieving UN Comtrade trade data, metadata, reference tables, analytical views, and downloadable files from Python.

This README describes the supplied **1.4.0 source files**. Most retrieval functions return a pandas DataFrame. Bulk and asynchronous download functions save files and return `None`. `getMBS(..., format_output="txt")` returns text.

## Installation

```bash
pip install comtradeapicall==1.4.0
```

For development, run the following from the repository root containing the packaging configuration:

```bash
pip install -e .
```

The supplied modules use `pandas`, `urllib3`, and `requests`. Use the Python version requirements and dependency versions declared in the release's packaging configuration. `python-dotenv` is optional for loading a private `.env` file.

## Quick start: no subscription key required

Australia's imports under HS chapter 91 in May 2022:

```python
import comtradeapicall

print("Imported library:", comtradeapicall.__file__)

df = comtradeapicall.previewFinalData(
    typeCode="C", freqCode="M", clCode="HS", period="202205",
    reporterCode="36", cmdCode="91", flowCode="M",
    partnerCode=None, partner2Code=None, customsCode=None, motCode=None,
    maxRecords=100, format_output="JSON", aggregateBy=None,
    breakdownMode="classic", countOnly=None, includeDesc=True,
)
print(df.head())
```

`maxRecords` requests a maximum, not a complete dataset. The ordinary retrieval functions make a single API call. For large selections, refine the filters or consider bulk files or asynchronous requests. Endpoint limits, quotas, availability, and subscription access are controlled by UN Comtrade.

## Optional subscription key and local configuration

Keep credentials outside shared code. Set `SUBSCRIPTION_KEY` in your environment, or install `python-dotenv`:

```bash
pip install python-dotenv
```

Create a private `.env` file:

```dotenv
SUBSCRIPTION_KEY=your_own_key
DIRECTORY=./comtrade_downloads
PROXY_URL=
```

Load the configuration:

```python
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path.cwd() / ".env", override=False)
subscription_key = (os.getenv("SUBSCRIPTION_KEY") or "").strip() or None
proxy_url = (os.getenv("PROXY_URL") or "").strip() or None
directory = str(Path(os.getenv("DIRECTORY") or "./comtrade_downloads").expanduser())
Path(directory).mkdir(parents=True, exist_ok=True)
```

Without `python-dotenv`, omit its import and the `load_dotenv` call and read environment variables directly.

Never commit a real key or `.env` file. Share a `.env.example` with a blank key instead:

```dotenv
SUBSCRIPTION_KEY=
DIRECTORY=./comtrade_downloads
PROXY_URL=
```

Suggested `.gitignore` entries:

```gitignore
.env
.env.*
!.env.example
comtrade_downloads/
__pycache__/
.ipynb_checkpoints/
```

### Public versus key-aware functions

- `previewFinalData`, `previewCountFinalData`, `previewTarifflineData`, `getWorldShare`, `getComtradeReleases`, `listReference`, `getReference`, and `convertCountryIso3ToCode` do not require a subscription-key argument.
- `getFinalData`, `getCountFinalData`, and `getTarifflineData` accept a required `subscription_key` argument. Passing `None` selects a public preview endpoint.
- `getFinalDataAvailability`, `getTarifflineDataAvailability`, and `getMetadata` select public endpoints when their required `subscription_key` argument is `None`.
- `getMBS` has an optional `subscription_key=None`; a non-`None` key selects the data endpoint.
- Bulk functions, asynchronous functions, `getLiveUpdate`, `getSUV`, `getTradeBalance`, and `getBilateralData` retain their bulk, async, data, or tools routes. Passing `None` does not switch them to a public route.
- For Trade Matrix retrieval and grouping, use a valid key with the intended access. In the supplied implementation, `getTradeMatrix(None, ...)` falls through to the ordinary public preview route with classification `TM`, not a dedicated public Trade Matrix route.

The wrappers' routing does not establish whether your account is entitled to a particular endpoint.

## Common parameters

| Parameter | Meaning | Example or note |
| --- | --- | --- |
| `typeCode` | Trade type | `"C"` goods; `"S"` services where supported |
| `freqCode` | Frequency | `"A"` annual; `"M"` monthly |
| `clCode` | Classification/revision | `"HS"`, `"S1"`; omitted from dedicated Trade Matrix wrappers |
| `period` | Reference year or year-month | `"2023"`, `"202205"` |
| `reporterCode` | Reporting country/area | `"36"` Australia |
| `cmdCode` | Commodity or aggregate | `"91"`, `"90,91"`, `"TOTAL"` |
| `flowCode` | Trade flow | `"M"` imports; `"X"` exports |
| `partnerCode` | Primary partner | `None` omits the filter; `"0"` explicitly selects code 0 |
| `partner2Code` | Secondary partner | Endpoint-specific |
| `customsCode` | Customs/statistical procedure | Endpoint-specific |
| `motCode` | Mode of transport | Endpoint-specific |
| `maxRecords` | Requested maximum rows | Not pagination or a completeness guarantee |
| `format_output` | Response format | Trade retrieval wrappers support `"JSON"` only; MBS supports `"json"`, `"csv"`, `"txt"` |
| `aggregateBy` | API aggregation option | Endpoint-specific |
| `breakdownMode` | Breakdown option | For example, `"classic"` |
| `countOnly` | Request counts instead of records | Prefer dedicated count functions |
| `includeDesc` | Include descriptions | `True` |
| `publishedDateFrom`, `publishedDateTo` | Publication filters | Not reference-period filters |
| `decompress` | Extract bulk gzip files | `False`: `.gz`; `True`: `.txt` |
| `showHistory` | Include metadata publication history | Required argument to `getMetadata` |
| `proxy_url` | Proxy address | Supported by all 41 public function signatures; see grouping caveat below |

Not every function accepts every parameter. Use keyword arguments and inspect signatures. Preserve leading zeros in commodity codes by using strings. `None` omits a filter; it is not a guaranteed world-total selection. Required parameters may still need to be passed explicitly as `None`.

## Function reference

The supplied package exports **41 public functions** through `__all__` and directly imports **seven legacy underscore helpers**.

### Preview and retrieval: 7 functions

| Function | Purpose |
| --- | --- |
| `previewFinalData` | Public preview of final data |
| `previewCountFinalData` | Public final-data count request |
| `previewTarifflineData` | Public tariff-line preview |
| `getFinalData` | Key-aware final-data retrieval |
| `getCountFinalData` | Key-aware final-data count request |
| `getTarifflineData` | Key-aware tariff-line retrieval |
| `getMBS` | Monthly Bulletin of Statistics data; JSON/CSV DataFrame or TXT text |

The dedicated count functions return a DataFrame containing a `count` column, not a plain integer. Trade retrieval wrappers support JSON only; requesting another format prints a message and returns `None`.

### Analytical views: 5 functions

| Function | Purpose |
| --- | --- |
| `getTradeBalance` | Trade-balance layout |
| `getBilateralData` | Reported data alongside mirror information |
| `getTradeMatrix` | Trade Matrix, which may contain estimated values |
| `getTradeMatrixGroup` | Group and aggregate Trade Matrix selections |
| `getWorldShare` | Public world-share data for reporting countries/areas |

`getTradeMatrix` does not accept `clCode`; it uses `TM` internally. Check descriptions and estimate indicators rather than assuming ordinary HS commodity semantics.

### Availability: 7 functions

| Function | Purpose |
| --- | --- |
| `getFinalDataAvailability` | Final dataset availability |
| `getTarifflineDataAvailability` | Tariff-line dataset availability |
| `getFinalDataBulkAvailability` | Final bulk-file availability |
| `getFinalClassicDataBulkAvailability` | Final-classic bulk-file availability |
| `getTarifflineDataBulkAvailability` | Tariff-line bulk-file availability |
| `getLiveUpdate` | Release information through the data endpoint; takes `subscription_key` |
| `getComtradeReleases` | Public release information; no subscription-key argument |

There is no separately exported Trade Matrix bulk-availability function. To inspect its bulk availability, use `getFinalDataBulkAvailability(..., clCode="TM")`, which uses the same bulk route as the dedicated Trade Matrix download wrappers.

### Bulk downloads: 12 functions

| Function | Purpose |
| --- | --- |
| `bulkDownloadFinalFile` | Download final files |
| `bulkDownloadFinalClassicFile` | Download final-classic files |
| `bulkDownloadTarifflineFile` | Download tariff-line files |
| `bulkDownloadTradeMatrixFile` | Download Trade Matrix files with classification fixed to `TM` |
| `bulkDownloadFinalFileDateRange` | Final files with publication-date filters |
| `bulkDownloadFinalClassicFileDateRange` | Final-classic files with publication-date filters |
| `bulkDownloadTarifflineFileDateRange` | Tariff-line files with publication-date filters |
| `bulkDownloadTradeMatrixFileDateRange` | Trade Matrix files with publication-date filters |
| `bulkDownloadAndCombineFinalFile` | Download and combine final files |
| `bulkDownloadAndCombineFinalClassicFile` | Download and combine final-classic files |
| `bulkDownloadAndCombineTarifflineFile` | Download and combine tariff-line files |
| `bulkDownloadAndCombineTradeMatrixFile` | Download and combine Trade Matrix files |

These functions save files and return `None`, not DataFrames or paths. The ordinary download functions also accept publication-date filters; the corresponding `DateRange` wrappers call the same implementation.

Combination functions download into a temporary directory, combine only the newly downloaded gzip files, retain one header, and reject inconsistent headers. They do not read or delete pre-existing files as sources. An existing destination with the same generated name can be replaced.

Use `decompress=False` for gzip output and `decompress=True` for text output. Gzip is not a ZIP archive.

#### Trade Matrix selections

The three dedicated Trade Matrix bulk wrappers do not accept `clCode`; it is always `"TM"`.

- `reporterCode="allcombined"` requests a combined-reporter selection.
- `period="allcombined"` requests a combined-period selection.
- Do not set both to `"allcombined"` in the same call.
- `"allcombined"` must be used alone, not inside a comma-separated list.
- Other bulk data types reject `"allcombined"`.
- `None` omits the relevant API filter and is distinct from explicitly requesting `"allcombined"`.

### Asynchronous requests: 5 functions

| Function | Purpose |
| --- | --- |
| `submitAsyncFinalDataRequest` | Submit final-data job; return request dictionary |
| `submitAsyncTarifflineDataRequest` | Submit tariff-line job; return request dictionary |
| `checkAsyncDataRequest` | Return status DataFrame, optionally filtered by batch ID |
| `downloadAsyncFinalDataRequest` | Submit a new final-data job, poll, and download |
| `downloadAsyncTarifflineDataRequest` | Submit a new tariff-line job, poll, and download |

The two download convenience functions submit **new jobs**; they do not reuse an earlier request ID. Both support:

- `poll_interval=15`: seconds between status checks.
- `max_wait_seconds=3600`: polling deadline in seconds.
- `proxy_url=None`: optional proxy.

Both polling settings must be positive. A polling deadline raises `TimeoutError`; invalid status responses and terminal failure states raise `RuntimeError`. The polling deadline is not a strict total-operation timeout: submission, individual HTTP calls, and the final file download have their own duration. Files retain the service-supplied filename, and the convenience functions return `None`.

### Metadata, references, and unit values: 5 functions

| Function | Purpose |
| --- | --- |
| `getMetadata` | Publication notes; `showHistory=True` includes history, `False` keeps latest publications |
| `listReference` | List reference tables; optional `category` filter |
| `getReference` | Retrieve a named reference table |
| `convertCountryIso3ToCode` | Convert comma-separated ISO3 codes to comma-separated Comtrade codes |
| `getSUV` | Standard Unit Values and related information |

`convertCountryIso3ToCode` strips whitespace and normalizes ISO3 inputs to uppercase. Results follow reference-table order, not necessarily input order; unmatched codes are omitted.

### Legacy helpers

- `_previewFinalData`
- `_previewTarifflineData`
- `_getFinalData`
- `_getTarifflineData`
- `_getFinalDataAvailability`
- `_getTarifflineDataAvailability`
- `_getMetadata`

These helpers remain directly accessible but are excluded from wildcard imports. The four trade-retrieval helpers split a comma-separated **string** of periods into individual calls and concatenate results. This is not general pagination and does not bypass per-call limits. They retry a transport failure once after a 10-second delay, not HTTP-status failures raised as `RuntimeError`.

The availability and metadata helpers use public endpoints without a key. `_getMetadata` does not accept `proxy_url`. Prefer public names for introductory examples.

## More examples

Run the import and configuration examples above first. Examples using authenticated routes require your own access. Check for `None` before calling DataFrame methods on functions that can return `None` after an error.

### References

```python
reference_tables = comtradeapicall.listReference()
reporters = comtradeapicall.getReference("reporter")
country_codes = comtradeapicall.convertCountryIso3ToCode("USA,FRA,CHE,ITA")
print(country_codes)
```

Use `listReference()` to discover available reference categories before requesting one.

### Authenticated final data

```python
if not subscription_key:
    raise ValueError("Configure SUBSCRIPTION_KEY first")

df = comtradeapicall.getFinalData(
    subscription_key=subscription_key,
    typeCode="C", freqCode="M", clCode="HS", period="202205",
    reporterCode="36", cmdCode="90,91", flowCode="M",
    partnerCode=None, partner2Code=None, customsCode=None, motCode=None,
    maxRecords=100, format_output="JSON", aggregateBy=None,
    breakdownMode="classic", countOnly=None, includeDesc=True,
    proxy_url=proxy_url,
)
print(df.head())
```

### Availability before downloading

```python
availability = comtradeapicall.getFinalDataBulkAvailability(
    subscription_key=subscription_key,
    typeCode="C", freqCode="M", clCode="HS",
    period="202205", reporterCode="36", proxy_url=proxy_url,
)
print(availability.head())

# Run separately after reviewing availability and access.
comtradeapicall.bulkDownloadFinalFile(
    subscription_key=subscription_key, directory=directory,
    typeCode="C", freqCode="M", clCode="HS",
    period="202205", reporterCode="36", decompress=False,
    proxy_url=proxy_url,
)
```

### Public World Share and release information

```python
world_share = comtradeapicall.getWorldShare(
    typeCode="C", freqCode="A", period="2023", reporterCode="36",
    proxy_url=proxy_url,
)
if world_share is not None:
    print(world_share.head())

releases = comtradeapicall.getComtradeReleases(proxy_url=proxy_url)
if releases is not None:
    print(releases.head())
```

Neither function accepts `subscription_key` or `clCode`.

### Monthly Bulletin of Statistics (MBS)

```python
import inspect
print(inspect.signature(comtradeapicall.getMBS))

# Set this to a valid MBS series identifier for your intended dataset.
series_type = os.getenv("MBS_SERIES_TYPE")
if series_type:
    mbs = comtradeapicall.getMBS(
        series_type=series_type, year="2023", format_output="json",
        proxy_url=proxy_url,
    )
    if mbs is not None:
        print(mbs.head())
```

`series_type` is required. Optional filters are `year`, `country_code`, `period`, `period_type`, and `table_type`. Their valid values depend on the MBS dataset. This example intentionally does not invent a series identifier. Omit the key for the public route, or pass `subscription_key=subscription_key` for the data route. JSON and CSV return DataFrames; TXT returns decoded text.

### Metadata history

```python
notes = comtradeapicall.getMetadata(
    subscription_key=subscription_key,
    typeCode="C", freqCode="M", clCode="HS",
    period="202205", reporterCode="36", showHistory=True,
    proxy_url=proxy_url,
)
print(notes.head())
```

### Trade Matrix grouping

```python
matrix = comtradeapicall.getTradeMatrix(
    subscription_key=subscription_key,
    typeCode="C", freqCode="A", period="2022",
    reporterCode="36", cmdCode="TOTAL", flowCode="X", partnerCode="0",
    includeDesc=True, proxy_url=proxy_url,
)

# '+' combines values into a group; ',' requests separate selections.
grouped = comtradeapicall.getTradeMatrixGroup(
    subscription_key=subscription_key,
    typeCode="C", freqCode="A", period="2022+2023,2024",
    reporterCode="36+554", cmdCode="TOTAL", flowCode="X", partnerCode="0",
    proxy_url=proxy_url,
)
print(grouped.head())
```

Named reporter and partner groups use the country-group definitions downloaded by the library. Grouping sums `primaryValue`. `isPrimaryValueEstimated` is `True` if any component is estimated, unknown if no component is known to be estimated but a flag is missing, and otherwise `False`.

Use explicit selections for `period`, `reporterCode`, `cmdCode`, and `partnerCode`; `None` or empty inputs produce no parsed selections and an empty result. Avoid overlapping members within a group because the parser does not deduplicate them.

The grouping helper checks for results of at least 100,000 rows and attempts to split multi-period selections. This is a hard-coded warning threshold, not a verified API limit or a guarantee of completeness. Individual-period results can still be truncated.

### Trade Matrix bulk downloads

```python
# All reporters combined for one year. No clCode argument.
comtradeapicall.bulkDownloadTradeMatrixFile(
    subscription_key=subscription_key, directory=directory,
    typeCode="C", freqCode="A", period="2023",
    reporterCode="allcombined", decompress=False, proxy_url=proxy_url,
)

# All periods combined for one reporter.
comtradeapicall.bulkDownloadTradeMatrixFile(
    subscription_key=subscription_key, directory=directory,
    typeCode="C", freqCode="A", period="allcombined",
    reporterCode="600", decompress=False, proxy_url=proxy_url,
)

# Download and locally combine files for separate reporters and periods.
comtradeapicall.bulkDownloadAndCombineTradeMatrixFile(
    subscription_key=subscription_key, directory=directory,
    typeCode="C", freqCode="A", period="2022,2023",
    reporterCode="36,554", decompress=False, proxy_url=proxy_url,
)
```

Publication filters can also be supplied to these functions or to `bulkDownloadTradeMatrixFileDateRange`.

### Async submission and status

```python
request = comtradeapicall.submitAsyncFinalDataRequest(
    subscription_key=subscription_key,
    typeCode="C", freqCode="M", clCode="HS",
    period="202205", reporterCode="36", cmdCode="90,91", flowCode="M",
    partnerCode=None, partner2Code=None, customsCode=None, motCode=None,
    aggregateBy=None, breakdownMode="classic", proxy_url=proxy_url,
)
if not isinstance(request, dict) or not request.get("requestId"):
    raise RuntimeError("No request ID returned")

status = comtradeapicall.checkAsyncDataRequest(
    subscription_key=subscription_key,
    batchId=request["requestId"], proxy_url=proxy_url,
)
print(status.head())
```

The job may still be processing. Rerun the status call later and inspect completion and the returned URI. Never use a historical hard-coded batch ID.

Alternatively, submit a **new** job and let the convenience function poll and download:

```python
comtradeapicall.downloadAsyncFinalDataRequest(
    subscription_key=subscription_key, directory=directory,
    typeCode="C", freqCode="M", clCode="HS",
    period="202205", reporterCode="36", cmdCode="90,91", flowCode="M",
    partnerCode=None, partner2Code=None, customsCode=None, motCode=None,
    aggregateBy=None, breakdownMode="classic", proxy_url=proxy_url,
    poll_interval=15, max_wait_seconds=3600,
)
```

## Proxies, errors, and troubleshooting

All 41 public functions accept `proxy_url`. For `getTradeMatrixGroup`, the proxy is forwarded to trade-data requests, but the country-group reference is fetched separately using `requests.get` without forwarding that argument. Configure the network environment accordingly.

```python
import inspect
print(inspect.signature(comtradeapicall.getFinalData))
print(inspect.signature(comtradeapicall.downloadAsyncFinalDataRequest))
```

- Check `comtradeapicall.__file__` if a local folder shadows an installed package.
- Empty results can be legitimate. Review reference-period and publication filters separately.
- A requested row maximum is not proof of completeness.
- Review key, entitlement, proxy, and network settings for failed requests.
- Error handling is not uniform: many functions raise exceptions, while `getWorldShare`, `getMBS`, and `getComtradeReleases` print some request errors and return `None`. Malformed responses may still raise exceptions.
- Trade retrieval wrappers also return `None` after printing a message for unsupported non-JSON formats.
- Bulk and async downloads validate HTTP status and use temporary files before publishing completed downloads.
- Use a writable dedicated download directory. Broad selections can create large files and long-running jobs.
- Existing generated output files can be replaced. Preserve important downloads separately.
- Never publish API keys or proxy credentials.

## Examples and tests

Examples and tests serve different purposes:

- **Learning notebook:** direct calls for exploring the library. If `comtradeapicall_getting_started.ipynb` is included, check its coverage against the 41 public functions above. The notebook was not supplied with these source files, so its coverage is not established here.
- **Live integration tests:** exercise real endpoints, network access, and subscription features.
- **Offline regression tests:** use mocked responses to check Python logic without live requests.

Clear notebook outputs before sharing. Exclude private configuration and large downloads from version control. Documentation examples do not replace live endpoint tests.

## Downloaded filenames

Individual bulk files use this pattern:

```text
COMTRADE-{DATA}-{TYPE}{FREQ}{REPORTER}{PERIOD}{CLASSIFICATION}[{RELEASE_DATE}].gz
```

`DATA` is `FINAL`, `FINALCLASSIC`, `TARIFFLINE`, or `TRADEMATRIX`. Reporter codes are padded to at least three characters. Classification and release date come from availability metadata. Tariff-line files use the metadata timestamp; other files use the publication date. Missing dates fall back to `1900-01-01` in the implementation.

Illustrative filenames:

```text
COMTRADE-FINAL-CM504200003H1[2023-01-03].gz
COMTRADE-TARIFFLINE-CM504200003H1[2023-01-03].gz
COMTRADE-TRADEMATRIX-CAALL2023TM[2026-11-03].gz
COMTRADE-TRADEMATRIX-CA600ALLTM[2026-11-03].gz
```

For individual Trade Matrix files, missing or combined reporter/period metadata is normalized to `ALL`, avoiding `None` or `nan` in the filename. These examples illustrate naming, not file availability.

Combined outputs use a different pattern:

```text
COMBINED-COMTRADE-{DATA}-{TYPE}{FREQ}{CLASSIFICATION}-{REPORTER_SELECTION}-{PERIOD_SELECTION}-[{LOCAL_DATE}].gz
```

The date is the local combination date, not a source publication date. `None` selections become `ALL`; other selections retain their string representation, with reporter selections padded to at least three characters. Explicit `allcombined` is retained in combined-output selection labels.

With `decompress=True`, the output extension is `.txt`. With `decompress=False`, the gzip header stores the final text filename with the same basename as the `.gz` output and a `.txt` extension, not the temporary name `combined.txt`. This applies to all four public combine wrappers.

Async downloads retain the filename supplied by the service.

## Documentation

- [UN Comtrade API developer portal](https://comtradeplus.un.org/MyComtrade/APIPortal)
- [UN Comtrade](https://comtrade.un.org)
- [UN Comtrade documentation](https://uncomtrade.org/docs)
- [Final versus tariff-line data](https://uncomtrade.org/docs/what-is-tariffline-data/)
- [Package on PyPI](https://pypi.org/project/comtradeapicall/)
- [Country-group definitions](https://comtradeapi.un.org/files/v1/app/reference/country_groups.json)

## Revision history

- **1.4.0:** Add Monthly Bulletin of Statistics retrieval, public World Share, public Comtrade data release information, and Trade Matrix bulk download/date-range/combine functions. Include bounded async polling, safer bulk combination and downloads, and grouping/metadata/reference handling improvements. Add a getting-started Jupyter notebook as described in the supplied release history; verify its coverage separately.
- **1.3.2:** Fix a bug in calculating Trade Matrix groups.
- **1.3.1:** Add a function to calculate Trade Matrix groupings.
- **1.3.0:** Add Trade Matrix extraction, complementing official statistics with estimates.
- **1.2.3:** Add functions to download and combine bulk files.
- **1.2.2:** Remove the unavailable AIS function; add trade-balance and bilateral views.

## Disclaimer

This package is provided "as is", without warranties or guarantees of any kind, including implied warranties of merchantability or fitness for a particular purpose. No support or maintenance is promised. This disclaimer does not replace the repository's license or applicable UN Comtrade data-use terms.
