using JitTopshelf.Dao;
using JitTopshelf.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace JitTopshelf.Repository
{
    public interface IWeatherRepository
    {
        List<string> GetDistinctZipCodes();
        DateTime GetEarliestDateNeededForZipWeather(int MoID, string ZipCode);
        int InsertWeatherData(WeatherData weatherData);
        bool InsertZipDate(string Zip, string RDate, int WdID);
        bool GetWeatherDataForZipDateExists(string ZipCode, string RDate);
        bool GetWeatherDataForStationAndDateExists(string StationID, string RDate);
        int GetWdIDFromWeatherData(string StationID, string RDate);
        int GetZipDateRowCount();
        int GetZipDateRowCountByZip(string ZipCode);
        int GetWeatherDataRowCountByZip(string ZipCode);
        DateTime GetMostRecentWeatherDataDate();
        List<ReadingsQueryResult> GetReadings(int MoID);
        List<ReadingsQueryResult> GetReadingsForExpUsageUpdate(int MoID, WthNormalParams normalParams);
        int GetExpectedWthExpUsageRowCount(int MoDI);
        int GetActualWthExpUsageRowCount();
        List<WeatherData> GetWeatherDataByZipStartAndEndDate(string ZipCode, DateTime DateStart, DateTime DateEnd);
        bool InsertWthExpUsage(int readingId, decimal value);

        List<WNRdngData> GetAllReadingsFromStoredProcedure();
        bool GetWthNormalParamsExists(WthNormalParams normalParams);
        bool InsertWthNormalParams(WthNormalParams normalParams);
        bool UpdateWthNormalParams(WthNormalParams normalParams);
        bool GetWthExpUsageExists(int RdngID);
        bool UpdateWthExpUsage(int RdngID, decimal value);

        void ClearWthNormalParams();
        List<WthNormalParams> GetAllParams();
        string GetBZip(int AccID, int UtilID, int UnitID);
    }
}
