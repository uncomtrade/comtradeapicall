# install the comtradeapicall first:
# py -m pip install comtradeapicall
# py -m pip install --upgrade comtradeapicall
# may need to install other dependencies
import comtradeapicall

subscription_key = '<YOUR KEY>' # comtrade api subscription key (from comtradedeveloper.un.org)
directory = '<OUTPUT DIR>'  # output directory for downloaded files
# Call preview final data API to a data frame, max to 500 records, no subscription key required
# This example: Australia imports of commodity code 91 in classic mode in May 2022
mydf = comtradeapicall.previewFinalData(typeCode='C', freqCode='M', clCode='HS', period='202205',
                                        reporterCode='36', cmdCode='91', flowCode='M', partnerCode=None,
                                        partner2Code=None,
                                        customsCode=None, motCode=None, maxRecords=500, format_output='JSON',
                                        aggregateBy=None, breakdownMode='classic', countOnly=None, includeDesc=True)
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
# Call bulk download tariff data file(s) to output dir, (premium) subscription key required
# This example: Download monthly France tariffline data of Jan-Mar 2000
comtradeapicall.bulkDownloadTarifflineFile(subscription_key, directory, typeCode='C', freqCode='M', clCode='HS',
                                           period='200001,200002,200003', reporterCode=504, decompress=True)
# Call bulk download tariff data file(s) to output dir, (premium) subscription key required
# This example: Download annual Morocco  data of 2010
comtradeapicall.bulkDownloadTarifflineFile(subscription_key, directory, typeCode='C', freqCode='A', clCode='HS',
                                           period='2010', reporterCode=504, decompress=True)
# Call bulk download tariff data file(s) to output dir, (premium) subscription key required
# This example: Download HS annual data released since  yesterday
from datetime import date
from datetime import timedelta
yesterday = date.today() - timedelta(days=1)
comtradeapicall.bulkDownloadFinalFileDateRange(subscription_key, directory, typeCode='C', freqCode='A',
                                               clCode='HS',
                                               period=None, reporterCode=None, decompress=False,
                                               publishedDateFrom=yesterday, publishedDateTo=None)
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
