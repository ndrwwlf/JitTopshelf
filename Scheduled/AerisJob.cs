using JitTopshelf.Dao;
using JitTopshelf.Model;
using JitTopshelf.Repository;
using Newtonsoft.Json;
using Quartz;
using Serilog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace JitTopshelf.Scheduled
{
    public class AerisJob : IJob
    {
        private AerisJobParams _aerisJobParams;
        private IWeatherRepository _weatherRepository;

        private int _expectedWthExpUsageInserts;
        private int _actualWthExpUsageInserts;
        
        private int _expectedHistoricalWeatherDataInsertsForZip;
        private int _actualHistoricalWeatherDataInsertsForZip;

        private int _expectedHistoricalZipDateInsertsForZip;
        private int _actualHistoricalZipDateInsertsForZip;

        private int _expectedZipDateEntriesTotal;
        private int _actualZipDateEntriesTotal;

        private int _actualWeatherDataEntriesTotal;

        private bool newZipsDailyGatherNeededForRegression;

        private readonly int _MoID = 301;

        List<string> _allZips = new List<string>();

        public void Execute(IJobExecutionContext context)
        {
            _aerisJobParams = AerisJobParamsValueOf(context);
            _weatherRepository = _weatherRepositoryValueOf(_aerisJobParams);

            Log.Information("WeatherData and ExpUsage job starting...\n");

            GatherWeatherData();

            PopulateWthExpUsageTable();

            Log.Information($"\nWeatherData and ExpUsage job finished. " +
                $"WeatherData was gathered and ExpUsage was calculated for Readings going back to MoID: {_MoID}.");
        }

        public void ExecuteZipHistoryCheckOnlyForRegression(IJobExecutionContext context)
        {
            _aerisJobParams = AerisJobParamsValueOf(context);
            _weatherRepository = _weatherRepositoryValueOf(_aerisJobParams);


            Log.Information("Checking if Historical WeatherData is needed...");

            try
            {
                _allZips = _weatherRepository.GetDistinctZipCodes();

                GatherHistoricalWeatherData(_allZips);

                if (newZipsDailyGatherNeededForRegression)
                {
                    GatherDailyWeatherData(-1, _allZips);

                    _actualZipDateEntriesTotal = _weatherRepository.GetZipDateRowCount();
                }
            }
            catch (Exception ex)
            {
                Log.Error(ex.Message);
                Log.Error(ex.StackTrace);
            }

            if (newZipsDailyGatherNeededForRegression)
            {
                Log.Information($"Finished GatherWeatherData(). " +
                    $"Expected Total ZipDates Entries: {_expectedZipDateEntriesTotal}.. Actual: {_actualZipDateEntriesTotal}.\n");
            }

            _expectedZipDateEntriesTotal = 0;
            _actualZipDateEntriesTotal = 0;
        }

        private void GatherWeatherData()
        {
            Log.Information("Checking if Historical WeatherData is needed...");

            try
            {
                _allZips = _weatherRepository.GetDistinctZipCodes();

                GatherHistoricalWeatherData(_allZips);

                //Log.Information("Starting Daily WeatherData calls...");
                GatherDailyWeatherData(-1, _allZips);

                _actualZipDateEntriesTotal = _weatherRepository.GetZipDateRowCount();
            }
            catch (Exception ex)
            {
                Log.Error(ex.Message);
                Log.Error(ex.StackTrace);
            }

            Log.Information($"Finished GatherWeatherData(). " +
                $"Expected Total ZipDates Entries: {_expectedZipDateEntriesTotal}, Actual: {_actualZipDateEntriesTotal}.\n");

            _expectedZipDateEntriesTotal = 0;
            _actualZipDateEntriesTotal = 0;
        }

        private void GatherDailyWeatherData(int i, List<string> zipCodes)
        {
            int expectedDailyZipDateInserts = 0;
            int actualDailyZipDateInserts= 0;
            int expectedDailyWeatherDataInserts = 0;
            int actualDailyWeatherDataInserts = 0;

            string targetDate = DateTime.Now.AddDays(i).ToShortDateString();

            if (zipCodes.Count > 1)
            {
                Log.Information($"Starting GatherDailyWeatherData(int {i}) for targetDate: {targetDate} and {zipCodes.Count} ZipCodes...");
            }
            else
            {
                Log.Information($"Starting GatherDailyWeatherData(int {i}) for targetDate: {targetDate}, Zip: {zipCodes.First().ToString()}...");
            }

            foreach (string zipCode in zipCodes)
            {
                if (!_weatherRepository.GetWeatherDataForZipDateExists(zipCode, targetDate))
                {
                    _expectedHistoricalZipDateInsertsForZip++;
                    expectedDailyZipDateInserts++;

                    try
                    {
                        WeatherData weatherData = BuildWeatherData(zipCode, targetDate);

                        if(!_weatherRepository.GetWeatherDataForStationAndDateExists(weatherData.StationID, targetDate))
                        {
                            expectedDailyWeatherDataInserts++;
                            _expectedHistoricalWeatherDataInsertsForZip++;
                            try
                            {
                                weatherData.WdID = _weatherRepository.InsertWeatherData(weatherData);
                            
                                Log.Debug($"Inserted into WeatherData >> WdID: {weatherData.WdID}, StationId: {weatherData.StationID}, RDate: {weatherData.RDate}," +
                                    $" LowTmp: {weatherData.LowTmp}, HighTmp: {weatherData.HighTmp}, AvgTmp: {weatherData.AvgTmp}.");

                                actualDailyWeatherDataInserts++;
                                _actualHistoricalWeatherDataInsertsForZip++;
                                _actualWeatherDataEntriesTotal++;
                            }
                            catch (Exception e)
                            {
                                throw new Exception($"Failed attempt: insert into WeatherData >> WdID: {weatherData.WdID}, StationId: {weatherData.StationID}, " +
                                    $"RDate: {weatherData.RDate}, LowTmp: {weatherData.LowTmp}, HighTmp: {weatherData.HighTmp}, " +
                                    $"AvgTmp: {weatherData.AvgTmp}. {e.Message}");
                            }

                            if (InsertZipDate(zipCode, targetDate, weatherData.WdID))
                            {
                                actualDailyZipDateInserts++;
                            }
                        }
                        else
                        {
                            int WdID = _weatherRepository.GetWdIDFromWeatherData(weatherData.StationID, targetDate);

                            if (InsertZipDate(zipCode, targetDate, WdID))
                            {
                                actualDailyZipDateInserts++;
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        Log.Error($"Zip: {zipCode}, TargetDate: {targetDate} >>  {e.Message}");
                        Log.Debug(e.StackTrace);
                    }
                }
            };

            if (zipCodes.Count > 1)
            {
                if (actualDailyWeatherDataInserts == expectedDailyWeatherDataInserts && actualDailyZipDateInserts == expectedDailyZipDateInserts)
                {
                    Log.Information($"Success >> Finished GatherDailyWeatherData for RDate: {targetDate}, " +
                        $"WeatherData Inserts: {actualDailyWeatherDataInserts}, ZipDates Inserts: {actualDailyZipDateInserts}.");
                }
                else
                {
                    Log.Warning($"Finished GatherDailyWeatherData for RDate: {targetDate}. " +
                        $"Expected WeatherData Inserts: {expectedDailyWeatherDataInserts}, Actual WeatherData Inserts: {actualDailyWeatherDataInserts}. \n" +
                        $"Expected ZipDate Inserts: {expectedDailyZipDateInserts}, Actual ZipDates Inserts: {actualDailyZipDateInserts}.");
                }
            }
            else
            {
                if (actualDailyWeatherDataInserts == expectedDailyWeatherDataInserts && actualDailyZipDateInserts == expectedDailyZipDateInserts)
                {
                    Log.Debug($"Success >> Finished GatherDailyWeatherData for Zip: {zipCodes.First().ToString()}, RDate: {targetDate}. " +
                        $"WeatherData Inserts: {actualDailyWeatherDataInserts}, ZipDates Inserts: {actualDailyZipDateInserts}.");
                }
                else
                {
                    Log.Warning($"Finished GatherDailyWeatherData for Zip: {zipCodes.First().ToString()} on RDate: {targetDate}, \n" +
                        $"Expected WeatherData Inserts: {expectedDailyWeatherDataInserts}, Actual: {actualDailyWeatherDataInserts} .. \n" +
                        $"Expected ZipDates Inserts: {expectedDailyZipDateInserts}, Actual: {actualDailyZipDateInserts}. \n");
                }
            }

            expectedDailyWeatherDataInserts = 0;
            actualDailyWeatherDataInserts = 0;
            expectedDailyZipDateInserts = 0;
            actualDailyZipDateInserts = 0;
        }

        private void GatherHistoricalWeatherData(List<string> zipCodes)
        {
            int expectedTotalWeatherDataInserts = 0;
            int actualTotalWeatherDataInserts = 0;

            int expectedTotalZipDateInserts = 0;
            int actualTotalZipDateInserts = 0;

            DateTime today = DateTime.Now;

            // yyyy, mm, dd
            //DateTime fromDate = new DateTime(2015, 01, 01);

            foreach (string Zip in zipCodes)
            {
                DateTime zipFromDateStart = _weatherRepository.GetEarliestDateNeededForZipWeather(_MoID, Zip);
                int days = (int)zipFromDateStart.Subtract(today).TotalDays;

                //int zipCount = _weatherRepository.GetDistinctZipCodes().Count;

                int expectedZipDateEntriesForZip = (days * -1) - 1;

                _expectedZipDateEntriesTotal += expectedZipDateEntriesForZip;

                //_actualTotalZipDateEntries = _weatherRepository.GetZipDateRowCount();
                int actualZipDateEntriesForZip = _weatherRepository.GetZipDateRowCountByZip(Zip);

                int expectedZipDateInsertsForZip = expectedZipDateEntriesForZip - actualZipDateEntriesForZip;

                if (expectedZipDateInsertsForZip > 0)
                {
                    newZipsDailyGatherNeededForRegression = true;

                    Log.Information($"Starting GatherHistoricalWeatherData() for Zip: {Zip}, from {zipFromDateStart.ToShortDateString()} to yesterday. {days} days.");

                    for (int i = days; i <= -1; i++)
                    {
                        GatherDailyWeatherData(i, new List<string>() { Zip });
                    };

                    //_expectedHistoricalZipDateInserts = _expectedTotalZipDateEntries - _actualTotalZipDateEntries + _allZips.Count;

                    Log.Information($"Finished GatherHistoricalWeatherData() for Zip: {Zip}. " +
                        $"Expected New WeatherData Inserts: {_expectedHistoricalWeatherDataInsertsForZip} Actual: {_actualHistoricalWeatherDataInsertsForZip}.\n" +
                        $"Expected ZipDates Inserts: {_expectedHistoricalZipDateInsertsForZip} Actual: {_actualHistoricalZipDateInsertsForZip}");

                    expectedTotalWeatherDataInserts += _expectedHistoricalWeatherDataInsertsForZip;
                    _expectedHistoricalWeatherDataInsertsForZip = 0;

                    expectedTotalZipDateInserts += _expectedHistoricalZipDateInsertsForZip;
                    _expectedHistoricalZipDateInsertsForZip = 0;

                    actualTotalWeatherDataInserts += _actualHistoricalWeatherDataInsertsForZip;
                    _actualHistoricalWeatherDataInsertsForZip = 0;

                    actualTotalZipDateInserts += _actualHistoricalZipDateInsertsForZip;
                    _actualHistoricalZipDateInsertsForZip = 0;
                }

                _expectedZipDateEntriesTotal += 1;
            }
        }

        private bool InsertZipDate(string zipCode, string targetDate, int WdID)
        {
            _expectedHistoricalZipDateInsertsForZip++;

            bool zipDateSuccess = _weatherRepository.InsertZipDate(zipCode, targetDate, WdID);

            if (zipDateSuccess)
            {
                _actualHistoricalZipDateInsertsForZip++;
                Log.Debug($"Inserted into ZipDates >> Zip: {zipCode}, {targetDate}, WdID: {WdID}");
            }
            else
            {
                Log.Error($"Failed Attempt: Insert into ZipDates >> Zip {zipCode}, {targetDate}, WdID: {WdID}");
            }

            return zipDateSuccess;
        }

        private void PopulateWthExpUsageTable()
        {
            Log.Information("Starting PopulateWthExpUsage()...");

            //string fromDateStartStr = $"{_fromDateStart.Month}-{_fromDateStart.Day}-{_fromDateStart.Year}";

            try
            {
                List<ReadingsQueryResult> readings = _weatherRepository.GetReadings(_MoID);

                _expectedWthExpUsageInserts = readings.Count;

                foreach (ReadingsQueryResult result in readings)
                {
                    try
                    {
                        if (!result.R2.HasValue
                            //|| result.R2.Value > 1 
                            //|| result.R2 < 0
                            )
                        {
                            continue;
                        }

                        if (result.R2.Value < 0.7500)
                        {
                            bool successAndNoModel = _weatherRepository.InsertWthExpUsage(result.RdngID, result.Units ?? 0);

                            if (successAndNoModel)
                            {
                                _actualWthExpUsageInserts++;
                                Log.Debug($"Inserted into WthExpUsage (No Weather Model) >> RdngID: {result.RdngID} ExpUsage: {result.Units ?? 0} << " +
                                            $"AccID/UtilID/UnitID: {result.AccID}/{result.UtilID}/{result.UnitID}, Actual Units: {result.Units}.");
                            }
                            else
                            {
                                Log.Error($"Failed attempt: Insert into WthExpUsage (No Weather Model) " +
                                            $">> RdngID: {result.RdngID} ExpUsage: {result.Units ?? 0} << " +
                                            $"AccID/UtilID/UnitID: {result.AccID}/{result.UtilID}/{result.UnitID}, Actual Units: {result.Units}");
                            }
                            continue;
                        }

                        if (result.DateStart == DateTime.MinValue || result.DateEnd == DateTime.MinValue)
                        {
                            throw new Exception("DateStart and/or DateEnd is null.");
                        }

                        int daysInReading = result.DateEnd.Subtract(result.DateStart).Days;

                        List<WeatherData> weatherDataList = _weatherRepository.GetWeatherDataByZipStartAndEndDate(result.Zip, result.DateStart, result.DateEnd);

                        if (weatherDataList.Count != daysInReading)
                        {
                            throw new Exception($"WeatherDataList.Count != daysInReading; WeatherDataList.Count = {weatherDataList.Count}, " +
                                $"daysInReading = {daysInReading}. Reading.StartDate = {result.DateStart} Reading.EndDate = {result.DateEnd}");
                        }

                        HeatingCoolingDegreeDays heatingCoolingDegreeDays = HeatingCoolingDegreeDaysValueOf(result, weatherDataList);

                        DoCalculation(result, heatingCoolingDegreeDays);
                    }
                    catch (Exception e)
                    {
                        Log.Error($"AccID/UtilID/UnitID: {result.AccID}/{result.UtilID}/{result.UnitID} >> " +
                            $"Cannot calculate ExpUsage for RdngID: {result.RdngID} >> {e.Message}");
                        Log.Debug(e.StackTrace);
                    }
                }

                int expectedTotalWthExpUsageEntries = _weatherRepository.GetExpectedWthExpUsageRowCount(_MoID);
                int actualTotalWthExpUsageEntries = _weatherRepository.GetActualWthExpUsageRowCount();

                Log.Information($"Finished PopulateWthExpUsage(). Expected inserts: {_expectedWthExpUsageInserts}, Actual: {_actualWthExpUsageInserts}");
                Log.Information($"Expected WthExpUsage total entries: {expectedTotalWthExpUsageEntries}, Actual: {actualTotalWthExpUsageEntries}.");

            }
            catch (Exception ex)
            {
                Log.Error($"Problem getting Readings >> {ex.Message} { ex.StackTrace}");
            }

            _expectedWthExpUsageInserts = 0;
            _actualWthExpUsageInserts = 0;
        }

        private void DoCalculation(ReadingsQueryResult result, HeatingCoolingDegreeDays heatingCoolingDegreeDays)
        {
            // Normalized Energy Usage = E = B1(DAYS) + B2(HDDB3) + B4(CDDB5)
            double? resultAsDouble = (result.B1 * result.Days) + (result.B2 * heatingCoolingDegreeDays.HDD) + (result.B4 * heatingCoolingDegreeDays.CDD);
            decimal resultAsDecimal = decimal.Round(Convert.ToDecimal(resultAsDouble), 4, MidpointRounding.AwayFromZero);

            bool success = _weatherRepository.InsertWthExpUsage(result.RdngID, resultAsDecimal);

            if (success)
            {
                Log.Debug($"Inserted into WthExpUsage >> RdngID: {result.RdngID} WthExpUsage: {resultAsDecimal} ... B1: {result.B1} B2: {result.B2} " +
                    $"B3: {result.B3} Hdd: {heatingCoolingDegreeDays.HDD} B4: {result.B4} B5: {result.B5} Cdd: {heatingCoolingDegreeDays.CDD} " +
                    $"AccID/UtilID/UnitID: {result.AccID}/{result.UtilID}/{result.UnitID}, R2: {result.R2}.");

                _actualWthExpUsageInserts++;
            }
            else
            {
                Log.Error($"FAILED attempt: insert into WthExpUsage >> RdngID: {result.RdngID} WthExpUsage: {resultAsDecimal} ... B1: {result.B1} B2: " +
                    $"{result.B2} B3: {result.B3} Hdd: {heatingCoolingDegreeDays.HDD} B4: {result.B4} B5: {result.B5} Cdd: {heatingCoolingDegreeDays.CDD} " +
                    $"AccID/UtilID/UnitID: {result.AccID}/{result.UtilID}/{result.UnitID}, R2: {result.R2}");
            }
        }

        private HeatingCoolingDegreeDays HeatingCoolingDegreeDaysValueOf(ReadingsQueryResult result, List<WeatherData> weatherDataList)
        {
            HeatingCoolingDegreeDays hcdd = new HeatingCoolingDegreeDays();
            hcdd.CDD = 0.0;
            hcdd.HDD = 0.0;

            if (result.B3 == 0 && result.B5 == 0)
            {
                return hcdd;
            }

            foreach (WeatherData weatherData in weatherDataList)
            {
                if (!weatherData.AvgTmp.HasValue)
                {
                    throw new Exception($"WeatherData.AvgTmp is null for {weatherData.ZipCode} on {weatherData.RDate}");
                }
                else if (result.B5 > 0)
                {
                    if (weatherData.AvgTmp >= result.B5)
                    {
                        hcdd.CDD = hcdd.CDD + (weatherData.AvgTmp.Value - result.B5);
                    }

                }
                else if (result.B3 > 0)
                {
                    if (weatherData.AvgTmp <= result.B3)
                    {
                        hcdd.HDD = hcdd.HDD + (result.B3 - weatherData.AvgTmp.Value);
                    }
                }
            }

            return hcdd;
        }

        private IWeatherRepository _weatherRepositoryValueOf(AerisJobParams aerisJobParams)
        {
            return new WeatherRepository(aerisJobParams);
        }

        private AerisJobParams AerisJobParamsValueOf(IJobExecutionContext context)
        {
            var schedulerContext = context.Scheduler.Context;
            return (AerisJobParams)schedulerContext.Get("aerisJobParams");
        }

        private WeatherData BuildWeatherData(string zipCode, string targetDate)
        {
            var targetDateTime = DateTime.Parse(targetDate);
            string aerisFromDate = targetDateTime.Date.ToString("MM/dd/yyyy");

            AerisResult result = GetAerisResult(zipCode, aerisFromDate);

            Response response = result.Response.First();
            Summary summary = response.Periods.First().Summary;

            Temp temp = summary.Temp;

            WeatherData weatherData = new WeatherData
            {
                StationID = response.Id,
                ZipCode = zipCode,
                RDate = targetDate,
                HighTmp = temp.MaxF,
                LowTmp = temp.MinF,
                AvgTmp = temp.AvgF
            };

            return weatherData;
        }

        private AerisResult GetAerisResult(string zipCode, string targetDate)
        {
            //string fromDate = targetDate.Date.ToString("MM/dd/yyyy");

            /* 
                * example
            http://api.aerisapi.com/observations/summary/closest?p=94304&query=maxt:!NULL,maxdewpt:!NULL&from=12/03/2014&to=12/03/2014&fields=id,periods.summary.dateTimeISO,periods.summary.temp.maxF,periods.summary.temp.minF,periods.summary.temp.avgF,periods.summary.dewpt.avgF
            */

            string rootUrl = $"http://api.aerisapi.com/observations/summary/closest?p={zipCode}&query=maxt:!NULL" +
                $"&from={targetDate}&to={targetDate}" +
                $"&fields=id,periods.summary.dateTimeISO,periods.summary.temp.maxF,periods.summary.temp.minF,periods.summary.temp.avgF";

            StringBuilder builder = new StringBuilder();
            builder.Append(rootUrl);
            builder.Append("&client_id=");
            builder.Append(_aerisJobParams.AerisClientId);
            builder.Append("&client_secret=");
            builder.Append(_aerisJobParams.AerisClientSecret);

            string url = builder.ToString();

            using (WebClient wc = new WebClient())
            {
                var json = wc.DownloadString(url);
                return JsonConvert.DeserializeObject<AerisResult>(json, new JsonSerializerSettings { NullValueHandling = NullValueHandling.Include });
            }
        }
    }
}
