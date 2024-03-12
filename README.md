# UN Comtrade API Package
This package simplifies calling [APIs of UN Comtrade](https://comtradedeveloper.un.org) to extract and download data
 (and much more). 

## Details
[UN Comtrade](https://comtrade.un.org) provides free and premium APIs to extract and download data/metadata, however
 it is quite a learning curve to understand all of APIs end-points and parameters. This package simplifies it by
  calling a single python function with the appropriate parameters. Learn more about UN Comtrade at the [UN Comtrade wiki](https://unstats.un.org/wiki/display/comtrade/UN+Comtrade).

This project is intended to be deployed at [The Python Package Index](https://pypi.org/project/comtradeapicall/), therefore the structure of
 folders follows the suggested layout from [Packaging Python Project](https://packaging.python.org/en/latest/tutorials/packaging-projects/). The main scripts are located at **/src/comtradeapicall/**. And the folder **tests** contains the example scripts how to install and use the package.
 
 ## Prerequisites
This package assumes using Python 3.7 and the expected package dependencies are listed in the "requirements.txt" file
 for PIP, you need to run the following command to get dependencies:
```
pip install -r requirements.txt
```

## Installing the package (from PyPi)
The package has been deployed to the PyPi and it can be install using pip command below:
```
pip install comtradeapicall
```

## Components
- **Get/Preview:** Model class to extract the data into pandas data frame
  - previewFinalData(**SelectionCriteria**, **query_option**) : return data frame containing final trade data (limited to 500 records)
  - previewTarifflineData(**SelectionCriteria**, **query_option**) : return data frame containing tariff line data (limited to 500
   records)
  - getFinalData(**subscription_key**, **SelectionCriteria**, **query_option**) : return data frame containing final
   trade data (limited to 250K records)
  - getTarifflineData(**subscription_key**, **SelectionCriteria**, **query_option**)  : return data frame containing
   tariff line data (limited to 250K records)
  - Alternative functions of _previewFinalData, _previewTarifflineData, _getFinalData, _getTarifflineData returns the
   same data frame, respectively,  with query optimization by calling multiple APIs based on the periods (instead of
    single API call)
   
- **DataAvailability:** Model class to extract data availability
  - _getFinalDataAvailability(**SelectionCriteria**) : return data frame containing final data
   availability - no subscription key
  - getFinalDataAvailability(**subscription_key**, **SelectionCriteria**) : return data frame containing final data
   availability
  - _getTarifflineDataAvailability(**SelectionCriteria**) : return data frame containing tariff
   line
   data
   availability - no subscription key
  - getTarifflineDataAvailability(**subscription_key**, **SelectionCriteria**) : return data frame containing tariff
   line
   data
   availability
  - getFinalDataBulkAvailability(**subscription_key**, **SelectionCriteria**, **[publishedDateFrom]**, **[publishedDateTo]**) : return data frame containing final bulk files data
   availability
  - getTarifflineDataBulkAvailability(**subscription_key**, **SelectionCriteria**, **[publishedDateFrom]**, **[publishedDateTo]**) : return data frame containing tariff
   line bulk files 
   data
   availability
  - getLiveUpdate(**subscription_key**) : return data frame recent data releases
  
- **BulkDownload:** Model class to download the data files
  - bulkDownloadFinalData(**subscription_key**, **directory**,  **SelectionCriteria**, **decompress**, **[publishedDateFrom]**, **[publishedDateTo]**) : download/save
   final data files to specified folder
  - bulkDownloadFinalClassicData(**subscription_key**, **directory**,  **SelectionCriteria**, **decompress**, **[publishedDateFrom]**, **[publishedDateTo]**) : download/save
   final classic data files to specified folder 
  - bulkDownloadTarifflineData(**subscription_key**, **directory**,  **SelectionCriteria**, **decompress**, **[publishedDateFrom]**, **[publishedDateTo]**) : download
  /save tariff line data files to specified folder

- **Async:** Model class to extract the data asynchronously (limited to 2.5M records) with email notification
  - submitAsyncFinalDataRequest(**subscription_key**, **SelectionCriteria**, **query_option**) : submit a final data job
  - submitAsyncTarifflineDataRequest(**subscription_key**, **SelectionCriteria**, **query_option**) : submit a tariff line data job
  - checkAsyncDataRequest(**subscription_key**, **[batchId]**) : check status of submitted job
  - downloadAsyncFinalDataRequest(**subscription_key**, **directory**, **SelectionCriteria**, **query_option**) : submit, wait and download the resulting final file
  - downloadAsyncTarifflineDataRequest(**subscription_key**, **directory**, **SelectionCriteria**, **query_option**) : submit, wait and download the resulting  tariff line file
 
- **Metadata:** Model class to extract metadata and publication notes
  - _getMetadata(**SelectionCriteria**, **showHistory**) : return data frame with metadata and publication notes - no subscription key
  - getMetadata(**subscription_key**, **SelectionCriteria**, **showHistory**) : return data frame with metadata and publication notes
  - listReference(**[category]**) : return data frame containing list of references
  - getReference(**category**) : return data frame with the contents of specific references

- **SUV:** Model class to extract data on Standard Unit Values (SUV) and their ranges
  - getSUV(**subscription_key**, **SelectionCriteria**, **[qtyUnitCode]**) : return data frame with SUV data

- **AIS:** Model class to extract experimental trade data generated from AIS (ships tracking movement). See [Cerdeiro, Komaromi, Liu and Saeed (2020)](https://www.imf.org/en/Publications/WP/Issues/2020/05/14/World-Seaborne-Trade-in-Real-Time-A-Proof-of-Concept-for-Building-AIS-based-Nowcasts-from-49393). *When consuming the data, users should understand its limitation.*  
  - getAIS(**subscription_key**, **AISSelectionCriteria**, **[vesselTypeCode]**) : return data frame with AIS trade data

See differences between final and tariff line data at the [Wiki](https://unstats.un.org/wiki/display/comtrade/New+Comtrade+FAQ+for+First+Time+Users#NewComtradeFAQforFirstTimeUsers-Whatisthetarifflinedata?)
 
## Selection Criteria
- typeCode(str) : Product type. Goods (C) or Services (S)
- freqCode(str) : The time interval at which observations occur. Annual (A) or Monthly (M)
- clCode(str) : Indicates the product classification used and which version (HS, SITC)
- period(str) :  Combination of year and month (for monthly), year for (annual)
- reporterCode(str) : The country or geographic area to which the measured statistical phenomenon relates
- cmdCode(str) : Product code in conjunction with classification code
- flowCode(str) : Trade flow or sub-flow (exports, re-exports, imports, re-imports, etc.)
- partnerCode(str) : The primary partner country or geographic area for the respective trade flow
- partner2Code(str) : A secondary partner country or geographic area for the respective trade flow
- customsCode(str) : Customs or statistical procedure
- motCode(str) : The mode of transport used when goods enter or leave the economic territory of a country

## Query Options
- maxRecords(int) : Limit number of returned records
- format_output(str) : The output format. CSV or JSON 
- aggregateBy(str) : Option for aggregating the query 
- breakdownMode(str) : Option to select the classic (trade by partner/product) or plus (extended breakdown) mode
- countOnly(bool) : Return the actual number of records if set to True 
- includeDesc(bool) : Option to include the description or not

## AIS Selection Criteria
- typeCode(str) : Product type. Only Goods (C)
- freqCode(str) : The time interval at which observations occur. Daily (D)
- datefrom(str) and dateto(str) :  Date(s) of observation - ASCII format
- countryareaCode(str) : The country or geographic area to which the measured statistical phenomenon relates. Use *getReference('ais:countriesareas')* for the complete list.
- vesselTypeCode(str) : The high level categorization of vessels transporting the goods. Use *getReference('ais:vesseltypes')* for the complete list.
- flowCode(str) : Trade flow (exports, imports)
 
## Examples of python usage
- Extract Australia imports of commodity code 91 in classic mode in May 2022
``` python
mydf = comtradeapicall.previewFinalData(typeCode='C', freqCode='M', clCode='HS', period='202205',
                                        reporterCode='36', cmdCode='91', flowCode='M', partnerCode=None,
                                        partner2Code=None,
                                        customsCode=None, motCode=None, maxRecords=500, format_output='JSON',
                                        aggregateBy=None, breakdownMode='classic', countOnly=None, includeDesc=True)
```    
- Extract Australia tariff line imports of commodity code started with 90 and 91 from Indonesia in May 2022
``` python
mydf = comtradeapicall.previewTarifflineData(typeCode='C', freqCode='M', clCode='HS', period='202205',
                                             reporterCode='36', cmdCode='91,90', flowCode='M', partnerCode=36,
                                             partner2Code=None,
                                             customsCode=None, motCode=None, maxRecords=500, format_output='JSON',
                                             countOnly=None, includeDesc=True)
```    
- Extract Australia imports of commodity codes 90 and 91 from all partners in classic mode in May 2022
``` python
mydf = comtradeapicall.getFinalData(subscription_key, typeCode='C', freqCode='M', clCode='HS', period='202205',
                                    reporterCode='36', cmdCode='91,90', flowCode='M', partnerCode=None,
                                    partner2Code=None,
                                    customsCode=None, motCode=None, maxRecords=2500, format_output='JSON',
                                    aggregateBy=None, breakdownMode='classic', countOnly=None, includeDesc=True)
```    
- Extract Australia tariff line imports of commodity code started with 90 and 91 from Indonesia in May 2022
``` python
mydf = comtradeapicall.getTarifflineData(subscription_key, typeCode='C', freqCode='M', clCode='HS', period='202205',
                                         reporterCode='36', cmdCode='91,90', flowCode='M', partnerCode=36,
                                         partner2Code=None,
                                         customsCode=None, motCode=None, maxRecords=2500, format_output='JSON',
                                         countOnly=None, includeDesc=True)
```  
- Download monthly France final data of Jan-2000
``` python
comtradeapicall.bulkDownloadFinalFile(subscription_key, directory, typeCode='C', freqCode='M', clCode='HS',
                                      period='200001', reporterCode=251, decompress=True)
```  
- Download monthly France tariff line data of Jan-March 2000
``` python
comtradeapicall.bulkDownloadTarifflineFile(subscription_key, directory, typeCode='C', freqCode='M', clCode='HS',
                                           period='200001,200002,200003', reporterCode=504, decompress=True)
```  
- Download annual Morocco tariff line  data of 2010
``` python
comtradeapicall.bulkDownloadTarifflineFile(subscription_key, directory, typeCode='C', freqCode='A', clCode='HS',
                                           period='2010', reporterCode=504, decompress=True)
```  
- Download all final annual data  in HS classification released yesterday
``` python
yesterday = date.today() - timedelta(days=1)
comtradeapicall.bulkDownloadTarifflineFile(subscription_key, directory, typeCode='C', freqCode='A', clCode='HS',
                                              period=None, reporterCode=None, decompress=True,
                                              publishedDateFrom=yesterday, publishedDateTo=None)
```  
- Show the recent releases
``` python
mydf = comtradeapicall.getLiveUpdate(subscription_key)
```  
- Extract final data availability in 2021
``` python
mydf = comtradeapicall.getFinalDataAvailability(subscription_key, typeCode='C', freqCode='A', clCode='HS',
                                                         period='2021', reporterCode=None)
```  
- Extract tariff line data availability in June 2022
``` python
mydf = comtradeapicall.getTarifflineDataAvailability(subscription_key, typeCode='C', freqCode='M', clCode='HS',
                                                        period='202206', reporterCode=None)
``` 
- Extract final bulk files data availability in 2021 for the SITC Rev.1 classification
``` python
mydf = comtradeapicall.getFinalDataBulkAvailability(subscription_key, typeCode='C', freqCode='A', clCode='S1',
                                                         period='2021', reporterCode=None)
``` 
- Extract tariff line bulk files data availability in June 2022
``` python
mydf = comtradeapicall.getTarifflineDataBulkAvailability(subscription_key, typeCode='C', freqCode='M', clCode='HS',
                                                        period='202206', reporterCode=None)
``` 
- List data availabity from last week for reference year 2021
``` python
mydf = comtradeapicall.getFinalDataAvailability(subscription_key, typeCode='C', freqCode='A', clCode='HS',period='2021', reporterCode=None, publishedDateFrom=lastweek, publishedDateTo=None)
``` 
- List tariffline data availabity from last week for reference period June 2022
``` python
mydf = comtradeapicall.getTarifflineDataAvailability(subscription_key, typeCode='C', freqCode='M',
                                                        clCode='HS',
                                                        period='202206', reporterCode=None, publishedDateFrom=lastweek, publishedDateTo=None)
``` 
- List bulk data availability for SITC Rev.1 for reference year 2021 released since last week
``` python
mydf = comtradeapicall.getFinalDataBulkAvailability(subscription_key, typeCode='C', freqCode='A',
                                                    clCode='S1',
                                                    period='2021', reporterCode=None, publishedDateFrom=lastweek, publishedDateTo=None)
``` 
- List bulk tariffline data availability from last week for reference period June 2022
``` python
mydf = comtradeapicall.getTarifflineDataBulkAvailability(subscription_key, typeCode='C', freqCode='M',
                                                            clCode='HS',
                                                            period='202206', reporterCode=None, publishedDateFrom=lastweek, publishedDateTo=None)

``` 
- Obtain all metadata and publication notes for May 2022
``` python
mydf = comtradeapicall.getMetadata(subscription_key, typeCode='C', freqCode='M', clCode='HS', period='202205',
                                                 reporterCode=None, showHistory=True)
``` 
- Submit asynchronous final data request
``` python
myJson = comtradeapicall.submitAsyncFinalDataRequest(subscription_key, typeCode='C', freqCode='M', clCode='HS',
                                    period='202205',
                                    reporterCode='36', cmdCode='91,90', flowCode='M', partnerCode=None,
                                    partner2Code=None,
                                    customsCode=None, motCode=None, aggregateBy=None, breakdownMode='classic')
print("requestID: ",myJson['requestId'])
``` 
- Submit asynchronous tariff line data request
``` python
myJson = comtradeapicall.submitAsyncTarifflineDataRequest(subscription_key, typeCode='C', freqCode='M',
                                                       clCode='HS',
                                          period='202205',
                                         reporterCode=None, cmdCode='91,90', flowCode='M', partnerCode=None,
                                         partner2Code=None,
                                         customsCode=None, motCode=None)
print("requestID: ",myJson['requestId'])
``` 
- Check status of asynchronous job
``` python
mydf = comtradeapicall.checkAsyncDataRequest(subscription_key, 
                                          batchId ='2f92dd59-9763-474c-b27c-4af9ce16d454' )
``` 
- Submit final data  asynchronous job and download the resulting file
``` python
comtradeapicall.downloadAsyncFinalDataRequest(subscription_key, directory,  typeCode='C', freqCode='M',
                                        clCode='HS', period='202209', reporterCode=None, cmdCode='91,90',
                                        flowCode='M', partnerCode=None, partner2Code=None,
                                        customsCode=None, motCode=None)
``` 
- Submit tariffline data  asynchronous job and download the resulting file
``` python
comtradeapicall.downloadAsyncTarifflineDataRequest(subscription_key, directory,  typeCode='C', freqCode='M',
                                        clCode='HS', period='202209', reporterCode=None, cmdCode='91,90',
                                        flowCode='M', partnerCode=None, partner2Code=None,
                                        customsCode=None, motCode=None)
``` 
- View list of reference tables
``` python
mydf = comtradeapicall.listReference()
mydf = comtradeapicall.listReference('cmd:B5')
``` 
- Download specific reference
``` python
mydf = comtradeapicall.getReference('reporter')
mydf = comtradeapicall.getReference('partner')
``` 
- Convert country/area ISO3 to Comtrade code
``` python
country_code = comtradeapicall.convertCountryIso3ToCode('USA,FRA,CHE,ITA')
``` 
- Get the Standard unit value (qtyUnitCode 8 [kg]) for commodity 010391 in 2022
``` python
mydf = comtradeapicall.getSUV(subscription_key, period='2022', cmdCode='010391', flowCode=None, qtyUnitCode=8)
``` 
- Get number of port calls and trade volume estimates derrived from AIS data for Australia between 1 and 8 February 2023 with vessel types bulk and container.
``` python
mydf = comtradeapicall.getAIS(subscription_key, countryareaCode=36, vesselTypeCode='1,2', dateFrom='2023-02-01', dateTo='2023-02-08')
``` 
- Tests folder contain more examples including calculation of unit value

## Downloaded file name convention
The naming convention follows the following : "COMTRADE-\<DATA>-\<TYPE>\<FREQ>\<COUNTRY CODE>\<YEAR\[
-MONTH\]>\<CLASSIFICATION CODE>\[\<RELEASE DATE\>\]"

As examples:
- Final merchandise trade data from Morocco (code 504) in March 2000 released on 3 Jan 2023 coded using H1
 classification:
  - *COMTRADE-FINAL-CM504200003H1[2023-01-03]*
- Tariffline merchandise trade from Morocco (code 504) in March 2000 released on 3 Jan 2023 coded using H1 classification: 
  - *COMTRADE-TARIFFLINE-CM504200003H1[2023-01-03]*

Note: Async download retains the original batch id










