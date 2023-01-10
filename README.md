# UN Comtrade API Package
This package simplifies calling [APIs of UN Comtrade](https://comtradedeveloper.un.org) to extract and download data
 (and much more)
. New users can learn more about UN Comtrade at the [wiki](https://unstats.un.org/wiki/display/comtrade/UN+Comtrade).

## Details
[UN Commtrade](https://comtrade.un.org) provides free and premium APIs to extract and download data/metadata, however
 it is quite a learning curve to understand all of APIs end-points and parameters. This package simplifies it by
  calling a single python function with the appropriate parameters. 

This project is intended to be deployed at [The Python Package Index](https://pypi.org/), therfore the structure of
 folders follows the suggested layout from [Packaging Python Project](https://packaging.python.org/en/latest/tutorials/packaging-projects/). The main script is located at **/src/comtradeapicall/__init__.py**. And the folder **tests** contains
 example
 script how to install and use the package.
 
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
  
- **BulkDownload:** Model class to download the data files
  - bulkDownloadFinalData(**subscription_key**, **directory**,  **SelectionCriteria**, **decompress**) : download/save
   final data files to specified folder
  - bulkDownloadTarifflineData(**subscription_key**, **directory**,  **SelectionCriteria**, **decompress**) : download
  /save tariff line data files to specified folder

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
- Download annual Morocco  data of 2010
``` python
comtradeapicall.bulkDownloadTarifflineFile(subscription_key, directory, typeCode='C', freqCode='A', clCode='HS',
                                           period='2010', reporterCode=504, decompress=True)
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










