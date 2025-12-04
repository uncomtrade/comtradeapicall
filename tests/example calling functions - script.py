# install the comtradeapicall first:
# py -m pip install comtradeapicall
# py -m pip install --upgrade comtradeapicall
# may need to install other dependencies
from datetime import timedelta
from datetime import date
import comtradeapicall

# set some variables
# comtrade api subscription key (from comtradedeveloper.un.org), some preview and metadata/reference API calls do not require key
subscription_key = '<YOUR KEY>'
directory = '<OUTPUT DIR>'  # output directory for downloaded files
proxy_url = '<PROXY URL>'  # optional if you need a proxy server

# set some variables again
today = date.today()
yesterday = today - timedelta(days=1)
lastweek = today - timedelta(days=7)


# Call preview final data API to a data frame, max to 500 records, no subscription key required
# This example: Australia imports of commodity code 91 in classic mode in May 2022
mydf = comtradeapicall.previewFinalData(typeCode='C', freqCode='M', clCode='HS', period='202205',
                                        reporterCode='36', cmdCode='91', flowCode='M', partnerCode=None,
                                        partner2Code=None,
                                        customsCode=None, motCode=None, maxRecords=500, format_output='JSON',
                                        aggregateBy=None, breakdownMode='classic', countOnly=None, includeDesc=True)
print(mydf.head(5))
# The same preview final call but using proxy_url
mydf = comtradeapicall.previewFinalData(typeCode='C', freqCode='M', clCode='HS', period='202205',
                                        reporterCode='36', cmdCode='91', flowCode='M', partnerCode=None,
                                        partner2Code=None,
                                        customsCode=None, motCode=None, maxRecords=500, format_output='JSON',
                                        aggregateBy=None, breakdownMode='classic', countOnly=None, includeDesc=True, proxy_url=proxy_url)
print(mydf.head(5))
# This function will split the query into multiple API calls for optimization (and avoiding timeout)
mydf = comtradeapicall._previewFinalData(typeCode='C', freqCode='M', clCode='HS', period='202105,202205',
                                         reporterCode='36', cmdCode='91', flowCode='M', partnerCode=None,
                                         partner2Code=None,
                                         customsCode=None, motCode=None, maxRecords=500, format_output='JSON',
                                         aggregateBy=None, breakdownMode='classic', countOnly=None, includeDesc=True)
print(mydf.head(5))

# Call preview tariffline data API to a data frame, max to 500 records, no subscription key required
# This example: Australia imports of commodity code started with 90 and 91 from Indonesia in May 2022
mydf = comtradeapicall.previewTarifflineData(typeCode='C', freqCode='M', clCode='HS', period='202205',
                                             reporterCode='36', cmdCode='91,90', flowCode='M', partnerCode=360,
                                             partner2Code=None,
                                             customsCode=None, motCode=None, maxRecords=500, format_output='JSON',
                                             countOnly=None, includeDesc=True)
print(mydf.head(5))
# This function will split the query into multiple API calls for optimization (and avoiding timeout)
mydf = comtradeapicall._previewTarifflineData(typeCode='C', freqCode='M', clCode='HS', period='202105,202205',
                                              reporterCode='36', cmdCode='91,90', flowCode='M', partnerCode=360,
                                              partner2Code=None,
                                              customsCode=None, motCode=None, maxRecords=500, format_output='JSON',
                                              countOnly=None, includeDesc=True)
print(mydf.head(5))

# Call get final data API to a data frame, max to 250K records, subscription key required
# This example: Australia imports of commodity codes 90 and 91 from all partners in classic mode in May 2022
mydf = comtradeapicall.getFinalData(subscription_key, typeCode='C', freqCode='M', clCode='HS', period='202205',
                                    reporterCode='36', cmdCode='91,90', flowCode='M', partnerCode=None,
                                    partner2Code=None,
                                    customsCode=None, motCode=None, maxRecords=2500, format_output='JSON',
                                    aggregateBy=None, breakdownMode='classic', countOnly=None, includeDesc=True)
print(mydf.head(5))
# This function will split the query into multiple API calls for optimization (and avoiding timeout)
mydf = comtradeapicall._getFinalData(subscription_key, typeCode='C', freqCode='M', clCode='HS', period='202105,202205',
                                     reporterCode='36', cmdCode='91,90', flowCode='M', partnerCode=None,
                                     partner2Code=None,
                                     customsCode=None, motCode=None, maxRecords=2500, format_output='JSON',
                                     aggregateBy=None, breakdownMode='classic', countOnly=None, includeDesc=True)
print(mydf.head(5))
# Call get tariffline data API to a data frame, max to 250K records, subscription key required
# This example: Australia imports of commodity code started with 90 and 91 from Indonesia in May 2022
mydf = comtradeapicall.getTarifflineData(subscription_key, typeCode='C', freqCode='M', clCode='HS', period='202205',
                                         reporterCode='36', cmdCode='91,90', flowCode='M', partnerCode=360,
                                         partner2Code=None,
                                         customsCode=None, motCode=None, maxRecords=2500, format_output='JSON',
                                         countOnly=None, includeDesc=True)
print(mydf.head(5))
# This function will split the query into multiple API calls for optimization (and avoiding timeout)
mydf = comtradeapicall._getTarifflineData(subscription_key, typeCode='C', freqCode='M', clCode='HS',
                                          period='202105,202205',
                                          reporterCode='36', cmdCode='91,90', flowCode='M', partnerCode=360,
                                          partner2Code=None,
                                          customsCode=None, motCode=None, maxRecords=2500, format_output='JSON',
                                          countOnly=None, includeDesc=True)
print(mydf.head(5))
# Call bulk download final file(s) API to output dir, (premium) subscription key required
# This example: Download monthly France final data of Jan-2000
comtradeapicall.bulkDownloadFinalFile(subscription_key, directory, typeCode='C', freqCode='M', clCode='HS',
                                      period='200001', reporterCode=251, decompress=True)
# Call bulk download final file(s) API to output dir, (premium) subscription key required
# This example: Download monthly France final classic data of Jan-2000
comtradeapicall.bulkDownloadFinalClassicFile(subscription_key, directory, typeCode='C', freqCode='M', clCode='HS',
                                             period='200001', reporterCode=251, decompress=True)
# Call bulk download tariff data file(s) to output dir, (premium) subscription key required
# This example: Download monthly France tariffline data of Jan-Mar 2000
comtradeapicall.bulkDownloadTarifflineFile(subscription_key, directory, typeCode='C', freqCode='M', clCode='HS',
                                           period='200001,200002,200003', reporterCode=504, decompress=True)
# Call bulk download tariff data file(s) to output dir, (premium) subscription key required
# This example: Download annual Morocco  data of 2010
comtradeapicall.bulkDownloadTarifflineFile(subscription_key, directory, typeCode='C', freqCode='A', clCode='HS',
                                           period='2010', reporterCode=504, decompress=True)
# Call bulk download tariff data file(s) to output dir, (premium) subscription key required
# This example: Download HS annual data released since  yesterday in three different sets Final, FinalClassic and Tariffline
yesterday = date.today() - timedelta(days=1)
# Download data in PLUS bulk file format
comtradeapicall.bulkDownloadFinalFileDateRange(subscription_key, directory, typeCode='C', freqCode='A',
                                               clCode='HS',
                                               period=None, reporterCode=None, decompress=False,
                                               publishedDateFrom=yesterday, publishedDateTo=None)
# Download data in CLASSIC bulk file format
comtradeapicall.bulkDownloadFinalClassicFile(subscription_key, directory, typeCode='C', freqCode='A',
                                             clCode='HS',
                                             period=None, reporterCode=None, decompress=False,
                                             publishedDateFrom=yesterday, publishedDateTo=None)
# Download data in TARIFFLINE bulk file format
comtradeapicall.bulkDownloadTarifflineFileDateRange(subscription_key, directory, typeCode='C', freqCode='A',
                                                    clCode='HS', period=None, reporterCode=None, decompress=False,
                                                    publishedDateFrom=yesterday, publishedDateTo=None)
# Call final data availability for annual HS in 2021
mydf = comtradeapicall.getFinalDataAvailability(subscription_key, typeCode='C', freqCode='A', clCode='HS',
                                                period='2021', reporterCode=None)
print(mydf.head(5))
# Call tariffline data availability for monthly HS in Jun-2022
mydf = comtradeapicall.getTarifflineDataAvailability(subscription_key, typeCode='C', freqCode='M', clCode='HS',
                                                     period='202206', reporterCode=None)
print(mydf.head(5))
# Call final bulk files data availability for annual S1 in 2021
mydf = comtradeapicall.getFinalDataBulkAvailability(subscription_key, typeCode='C', freqCode='A', clCode='S1',
                                                    period='2021', reporterCode=None)
print(mydf.head(5))
print(len(mydf))
# Call tariffline bulk files data availability for monthly HS in Jun-2022
mydf = comtradeapicall.getTarifflineDataBulkAvailability(subscription_key, typeCode='C', freqCode='M', clCode='HS',
                                                         period='202206', reporterCode=None)
print(mydf.head(5))
print(len(mydf))
# Call live update
mydf = comtradeapicall.getLiveUpdate(subscription_key)
print(mydf.head(5))
print(len(mydf))
# Get metadata
mydf = comtradeapicall.getMetadata(subscription_key, typeCode='C', freqCode='M', clCode='HS', period='202205',
                                   reporterCode=None, showHistory=False)
print(mydf.head(5))
print(len(mydf))
# Get metadata without subscription key
mydf = comtradeapicall._getMetadata(typeCode='C', freqCode='M', clCode='HS', period='202205',
                                    reporterCode=None, showHistory=False)
print(mydf.head(5))
print(len(mydf))
# Submit async request (final data)
myJson = comtradeapicall.submitAsyncFinalDataRequest(subscription_key, typeCode='C', freqCode='M', clCode='HS',
                                                     period='202205',
                                                     reporterCode='36', cmdCode='91,90', flowCode='M', partnerCode=None,
                                                     partner2Code=None,
                                                     customsCode=None, motCode=None, aggregateBy=None,
                                                     breakdownMode='classic')
print("requestID: ", myJson['requestId'])
# Submit async request (tariffline data)
myJson = comtradeapicall.submitAsyncTarifflineDataRequest(subscription_key, typeCode='C', freqCode='M',
                                                          clCode='HS',
                                                          period='202205',
                                                          reporterCode=None, cmdCode='91,90', flowCode='M',
                                                          partnerCode=None,
                                                          partner2Code=None,
                                                          customsCode=None, motCode=None)
print("requestID: ", myJson['requestId'])
# check async status
mydf = comtradeapicall.checkAsyncDataRequest(subscription_key,
                                             batchId='2f92dd59-9763-474c-b27c-4af9ce16d454')
print(mydf.iloc[0]['status'])
print(mydf.iloc[0]['uri'])
mydf = comtradeapicall.checkAsyncDataRequest(subscription_key)
print(len(mydf))
# submit async and download the result (final data)
comtradeapicall.downloadAsyncFinalDataRequest(subscription_key, directory, typeCode='C', freqCode='M',
                                              clCode='HS', period='202209', reporterCode=None, cmdCode='91,90',
                                              flowCode='M', partnerCode=None, partner2Code=None,
                                              customsCode=None, motCode=None)
# submit async and download the result (tariffline)
comtradeapicall.downloadAsyncTarifflineDataRequest(subscription_key, directory, typeCode='C', freqCode='M',
                                                   clCode='HS', period='202209', reporterCode=None, cmdCode='91,90',
                                                   flowCode='M', partnerCode=None, partner2Code=None,
                                                   customsCode=None, motCode=None)
# download the list of reference tables
mydf = comtradeapicall.listReference()
print(mydf.head(5))
print(len(mydf))
mydf = comtradeapicall.listReference('cmd:B5')
print(mydf.head(5))
print(len(mydf))
# download specific reference (list available at listReference())
mydf = comtradeapicall.getReference('reporter')
print(mydf.head(5))
print(len(mydf))
mydf = comtradeapicall.getReference('partner')
print(mydf.head(5))
print(len(mydf))
# Convert country/area ISO3 to Comtrade code
country_code = comtradeapicall.convertCountryIso3ToCode('USA,FRA,CHE,ITA')
print(country_code)
# use the convert function country_code in preview call
mydf = comtradeapicall.previewFinalData(typeCode='C', freqCode='M', clCode='HS', period='202205',
                                        reporterCode=comtradeapicall.convertCountryIso3ToCode('USA,FRA,CHE,ITA'), cmdCode='91', flowCode='M', partnerCode=None,
                                        partner2Code=None, customsCode=None, motCode=None)
print(mydf.head(5))
# list data availabity from last week for reference year 2021
mydf = comtradeapicall.getFinalDataAvailability(subscription_key, typeCode='C', freqCode='A', clCode='HS',
                                                period='2021', reporterCode=None, publishedDateFrom=lastweek, publishedDateTo=None)
print(mydf.head(5))
print(len(mydf))
# list data availabity from last week for reference year 2021 without subscription key
mydf = comtradeapicall._getFinalDataAvailability(typeCode='C', freqCode='A', clCode='HS',
                                                 period='2021', reporterCode=None, publishedDateFrom=lastweek, publishedDateTo=None)
print(mydf.head(5))
print(len(mydf))
# list tariffline data availabity from last week for reference period June 2022
mydf = comtradeapicall.getTarifflineDataAvailability(subscription_key, typeCode='C', freqCode='M',
                                                     clCode='HS',
                                                     period='202206', reporterCode=None, publishedDateFrom=lastweek, publishedDateTo=None)
print(mydf.head(5))
print(len(mydf))
# list tariffline data availabity from last week for reference period June 2022 without subscription key
mydf = comtradeapicall._getTarifflineDataAvailability(typeCode='C', freqCode='M',
                                                      clCode='HS',
                                                      period='202206', reporterCode=None, publishedDateFrom=lastweek, publishedDateTo=None)
print(mydf.head(5))
print(len(mydf))
# list bulk data availability for SITC Rev.1 for reference year 2021 released since last week
mydf = comtradeapicall.getFinalDataBulkAvailability(subscription_key, typeCode='C', freqCode='A',
                                                    clCode='S1',
                                                    period='2021', reporterCode=None, publishedDateFrom=lastweek, publishedDateTo=None)
print(mydf.head(5))
print(len(mydf))
# list bulk tariffline data availability from last week for reference period June 2022
mydf = comtradeapicall.getTarifflineDataBulkAvailability(subscription_key, typeCode='C', freqCode='M',
                                                         clCode='HS',
                                                         period='202206', reporterCode=None, publishedDateFrom=lastweek, publishedDateTo=None)
print(mydf.head(5))
print(len(mydf))
# Get the Standard unit value (qtyUnitCode 8 [kg]) for commodity 010391 in 2022
mydf = comtradeapicall.getSUV(subscription_key,
                              period='2022', cmdCode='010391', flowCode=None, qtyUnitCode=8)
print(mydf.head(5))
print(len(mydf))
# Get data in trade balance layout (exports and imports next to each other)
mydf = comtradeapicall.getTradeBalance(subscription_key, typeCode='C', freqCode='M', clCode='HS', period='202205',
                                       reporterCode='36', cmdCode='TOTAL', partnerCode=None)
print(mydf.head(5))
# Get data in bilateral layout (basic data is complemented by mirror partner data)
mydf = comtradeapicall.getBilateralData(subscription_key, typeCode='C', freqCode='M', clCode='HS', period='202205',
                                        reporterCode='36', cmdCode='TOTAL', flowCode='X', partnerCode=None)
print(mydf.head(5))
# Count of numbers of records (without key - single period)
mydf = comtradeapicall.previewCountFinalData(typeCode='C', freqCode='M', clCode='HS', period='202201', reporterCode='', cmdCode='9*', flowCode='M', partnerCode='0,826',
                                             partner2Code=None, customsCode=None, motCode=None, aggregateBy=None, breakdownMode='classic')
print(mydf.head(5))
# Count of numbers of records (using subscription - multiple periods)
mydf = comtradeapicall.getCountFinalData(subscription_key, typeCode='C', freqCode='M', clCode='HS', period='202201,202202', reporterCode='', cmdCode='9*', flowCode='M', partnerCode='0,826',
                                         partner2Code=None, customsCode=None, motCode=None, aggregateBy=None, breakdownMode='classic')
print(mydf.head(5))
# download and combine monthly files in Final Classic type published in 7 days
comtradeapicall.bulkDownloadAndCombineFinalClassicFile(
    subscription_key, directory=directory,  typeCode='C', freqCode='M', clCode='HS', period=None, reporterCode=None, decompress=False, publishedDateFrom=lastweek, publishedDateTo=None)
# download and combine all tariff line data in 2022 into single file
comtradeapicall.bulkDownloadAndCombineTarifflineFile(
    subscription_key, directory=directory, typeCode='C', freqCode='A', clCode='HS', period='2022', reporterCode=None, decompress=False)
# download and combine all annual final data for Guyana (code 328) from all years into single file
comtradeapicall.bulkDownloadAndCombineFinalFile(
    subscription_key, directory=directory,  typeCode='C', freqCode='A', clCode='HS', period=None, reporterCode=328, decompress=False)
