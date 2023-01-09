# ComtradeAPICALL Package
This package simplifies calling [APIs of UN Comtrade](https://comtradedeveloper.un.org) to extract and download data
. New users can learn more about UN Comtrade at the [wiki](https://unstats.un.org/wiki/display/comtrade/UN+Comtrade).

The main script is located at **/src/comtradeapicall/__init__.py**. And the folder **tests** contains
 example
 script how to install and use the package.

The packaging instruction is located at [Packaging Python Project](https://packaging.python.org/en/latest/tutorials/packaging-projects/).

##Available functions:
- *previewFinalData, previewTarifflineData* (extract data to pandas data frame, max record 500, no subscription key)
- *getFinalData, getTarifflineData* (extract data to pandas data frame, max record 250K, require free or premium
 subscription
 key)
- *bulkDownloadFinalFile, bulkDownloadTarifflineFile* (download data files, require premium subscription
 key)
 
(See tests folder for examples)

###Downloaded file name
The naming convention follows the following : "COMTRADE-\<DATA>-\<TYPE>\<FREQ>\<COUNTRY CODE>\<YEAR\[
-MONTH\]>\<CLASSIFICATION CODE>\[\<RELEASE DATE\>\]"

As examples:
- Final merchandise trade data from Morocco (code 504) in March 2000 released on 3 Jan 2023 coded using H1
 classification
 : *COMTRADE
-FINAL-CM504200003H1[2023-01-03]*
- Tariffline merchandise trade from Morocco (code 504) in March 2000 released on 3 Jan 2023 coded using H1 classification: *COMTRADE
-TARIFFLINE-CM504200003H1[2023-01-03]*

See differences between final and tariffline data at the [Wiki](https://unstats.un.org/wiki/display/comtrade/New+Comtrade+FAQ+for+First+Time+Users#NewComtradeFAQforFirstTimeUsers-Whatisthetarifflinedata?)
