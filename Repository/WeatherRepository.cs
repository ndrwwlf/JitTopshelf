using Dapper;
using JitTopshelf.Dao;
using JitTopshelf.Model;
using JitTopshelf.Scheduled;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace JitTopshelf.Repository
{
    public class WeatherRepository : IWeatherRepository
    {
        private readonly string _jitWeatherConnectionString;
        private readonly string _jitWebData3ConnectionString;

        public WeatherRepository(AerisJobParams aerisJobParams)
        {
            _jitWeatherConnectionString = aerisJobParams.JitWeatherConnectionString;
            _jitWebData3ConnectionString = aerisJobParams.JitWebData3ConnectionString;
        }

        public List<string> GetDistinctZipCodes()
        {
            List<string> allZips = new List<string>();

            using (IDbConnection db = new SqlConnection(_jitWebData3ConnectionString))
            {
                allZips = db.Query<string>("select distinct b.Zip from Buildings as b " +
                    "join Accounts as a on b.BldID = a.BldID " +
                    "join WthNormalParams as w on a.AccID = w.AccID").AsList();
            }
            return allZips;
        }

        public bool InsertWeatherData(WeatherData weatherData)
        {
            string sql = @"
            INSERT INTO [WeatherData] ([StationId], [ZipCode], [RDate], [HighTmp], [LowTmp], [AvgTmp], [DewPt]) 
            VALUES (@StationId, @ZipCode, @RDate, @HighTmp, @LowTmp, @AvgTmp, @DewPT);
            SELECT CAST(SCOPE_IDENTITY() as int)";

            using (IDbConnection db = new SqlConnection(_jitWeatherConnectionString))
            {
                int rowsAffected = db.Execute(sql, new
                {
                    StationID = weatherData.StationId,
                    ZipCode = weatherData.ZipCode,
                    RDate = weatherData.RDate.ToShortDateString(),
                    HighTmp = weatherData.HighTmp,
                    LowTmp = weatherData.LowTmp,
                    AvgTmp = weatherData.AvgTmp,
                    DewPT = weatherData.DewPt
                });

                return (rowsAffected == 1);
            }
        }

        public bool GetWeatherDataExistForZipAndDate(string zipCode, DateTime rDate)
        {
            bool exists;

            using (IDbConnection db = new SqlConnection(_jitWeatherConnectionString))
            {
                DateTime date = Convert.ToDateTime(rDate.ToShortDateString());
                exists = db.ExecuteScalar<bool>("select count(1) from WeatherData where ZipCode=@ZipCode AND RDate=@RDate",
                    new { ZipCode = zipCode, RDate = date });
            }

            return exists;
        }


        public int GetWeatherDataRowCount()
        {
            string sql = @"SELECT COUNT(ID) FROM [WeatherData] WHERE ZipCode IS NOT NULL";
            using (IDbConnection db = new SqlConnection(_jitWeatherConnectionString))
            {
                return db.ExecuteScalar<int>(sql);
            }
        }

        public int GetWeatherDataRowCountByZip(string ZipCode)
        {
            var sql = @"SELECT COUNT(*) FROM WeatherData WHERE ZipCode = @ZipCode";

            using (IDbConnection db = new SqlConnection(_jitWeatherConnectionString))
            {
                return db.ExecuteScalar<int>(sql, new { ZipCode });
            }
        }

        public DateTime GetMostRecentWeatherDataDate()
        {
            using (IDbConnection db = new SqlConnection(_jitWeatherConnectionString))
            {
                var date = db.Query<DateTime>("SELECT TOP(1) RDate FROM WeatherData ORDER BY RDate DESC").First();
                return date;
            }
        }

        public List<ReadingsQueryResult> GetReadings(string DateStart)
        {
            string DateEnd = GetMostRecentWeatherDataDate().AddDays(1).ToShortDateString();
            var data = new List<ReadingsQueryResult>();

            string Sql = @"select r.RdngID, b.Zip, r.DateStart, r.DateEnd, r.Days, r.UnitID as rUnitID, 
                                  wnp.UnitID as wnpUnitID, wnp.B1, wnp.B2, wnp.B3, wnp.B4, wnp.B5
                            from Readings r 
                               join WthNormalParams wnp on wnp.AccID = r.AccID
                                                        and wnp.UtilID = r.UtilID
                                                        and wnp.UnitID = r.UnitID
                            join Accounts a on a.AccID = r.AccID
                            join Buildings b on b.BldID = a.BldID
                            where not exists 
                                (select weu.RdngID from WthExpUsage weu
                                    where weu.RdngID = r.RdngID)
                            and r.DateStart >= @DateStart
                            and r.DateEnd <= @DateEnd
                            order by DateStart asc";

            using (IDbConnection db = new SqlConnection(_jitWebData3ConnectionString))
            {
                return db.Query<ReadingsQueryResult>(Sql, new { DateStart, DateEnd }).AsList();
            }
        }

        public List<ReadingsQueryResult> GetReadingsForExpUsageUpdate(string DateStart, WthNormalParams normalParams)
        {
            string DateEnd = GetMostRecentWeatherDataDate().AddDays(1).ToShortDateString();
            var data = new List<ReadingsQueryResult>();

            string Sql = @"select r.RdngID, b.Zip, r.DateStart, r.DateEnd, r.Days, r.UnitID as rUnitID, 
                                  wnp.UnitID as wnpUnitID, wnp.B1, wnp.B2, wnp.B3, wnp.B4, wnp.B5
                            from Readings r 
                               join WthNormalParams wnp on wnp.AccID = r.AccID
                                                        and wnp.UtilID = r.UtilID
                                                        and wnp.UnitID = r.UnitID
                            join Accounts a on a.AccID = r.AccID
                            join Buildings b on b.BldID = a.BldID
                            where 
                            wnp.AccID = @AccID and
                            wnp.UtilID = @UtilID and
                            wnp.UnitID = @UnitID and
                            r.DateStart >= @DateStart and
                            r.DateEnd <= @DateEnd
                            order by DateStart asc";

            using (IDbConnection db = new SqlConnection(_jitWebData3ConnectionString))
            {
                return db.Query<ReadingsQueryResult>(Sql, new { DateStart, DateEnd }).AsList();
            }
        }

        public int GetExpectedWthExpUsageRowCount(string DateStart)
        {
            string DateEnd = GetMostRecentWeatherDataDate().AddDays(1).ToShortDateString();

            string sql = @"select count(r.RdngID) 
                           from Readings r 
                           join WthNormalParams wnp on wnp.AccID = r.AccID
                                                    and wnp.UtilID = r.UtilID
                                                    and wnp.UnitID = r.UnitID
                           join Accounts a on a.AccID = r.AccID
                           join Buildings b on b.BldID = a.BldID
                           where  r.DateStart >= @DateStart
                              and r.DateEnd <= @DateEnd";

            using (IDbConnection db = new SqlConnection(_jitWebData3ConnectionString))
            {
                return db.ExecuteScalar<int>(sql, new { DateStart, DateEnd });
            }
        }

        public int GetActualWthExpUsageRowCount()
        {
            string sql = @"SELECT COUNT(RdngID) FROM [WthExpUsage]";
            using (IDbConnection db = new SqlConnection(_jitWebData3ConnectionString))
            {
                return db.ExecuteScalar<int>(sql);
            }
        }

        public List<WeatherData> GetWeatherDataByZipStartAndEndDate(string ZipCode, DateTime DateStart, DateTime DateEnd)
        {
            var data = new List<WeatherData>();

            string Sql = @"SELECT ID, (RTRIM(StationId)) as StationId, (RTRIM(ZipCode)) as ZipCode, RDate, HighTmp, LowTmp, AvgTmp, DewPt FROM WeatherData  
                             WHERE ZipCode = @ZipCode  AND RDATE >= @DateStart AND RDATE < @DateEnd ORDER BY ID";

            using (IDbConnection db = new SqlConnection(_jitWeatherConnectionString))
            {
                return db.Query<WeatherData>(Sql, new { ZipCode, DateStart, DateEnd }).AsList();
            }
        }

        public bool GetWthExpUsageExists(int RdngID)
        {
            bool exists;
            string sql = "select count(1) from WthExpUsage where RdngID = @RdngID";

            using (IDbConnection db = new SqlConnection(_jitWebData3ConnectionString))
            {
                exists = db.ExecuteScalar<bool>(sql, new { RdngID });
            }
            return exists;
        }

        public bool InsertWthExpUsage(int ReadingId, decimal ExpUsage)
        {
            string sql = @"
            INSERT INTO [WthExpUsage] ([RdngID], [ExpUsage]) 
            VALUES (@ReadingID, @ExpUsage);
            SELECT CAST(SCOPE_IDENTITY() as int)";

            using (IDbConnection db = new SqlConnection(_jitWebData3ConnectionString))
            {
                int rowsAffected = db.Execute(sql, new
                {
                    ReadingId,
                    ExpUsage
                });

                return (rowsAffected == 1);
            }
        }

        public bool UpdateWthExpUsage(int RdngID, decimal ExpUsage)
        {
            string sql = @"Update [WthExpUsage] set ExpUsage = @ExpUsage where RdngID = @RdngID";
            int rowsAffected = 0;

            using (IDbConnection db = new SqlConnection(_jitWebData3ConnectionString))
            {
                rowsAffected = db.Execute(sql, new
                { ExpUsage, RdngID });
            }

            return (rowsAffected == 1);
        }

        public List<WNRdngData> GetAllReadingsFromStoredProcedure()
        {
            using (IDbConnection db = new SqlConnection(_jitWebData3ConnectionString))
            {
                return db.Query<WNRdngData>("WNRdngData01", CommandType.StoredProcedure).AsList();
            }
        }

        public bool GetWthNormalParamsExists(WthNormalParams normalParams)
        {
            bool exists;
            string sql = "select count(1) from WthNormalParams where AccID = @AccID and UtilID = @UtilID and UnitID = @UnitID";

            using (IDbConnection db = new SqlConnection(_jitWebData3ConnectionString))
            {
                exists = db.ExecuteScalar<bool>(sql, new { normalParams.AccID, normalParams.UtilID, normalParams.UnitID });
            }
            return exists;
        }

        public bool InsertWthNormalParams(WthNormalParams normalParams)
        {
            int rowsAffected = 0;
            string sql = @"
                INSERT INTO [WthNormalParams] (
                [AccID], [UtilID], [UnitID], [WstID], [ZipW], 
                [B1], [B2], [B3], [B4], [B5], [R2], 
                [EndDate], [EMoID], [MoCt]
                ) 
                VALUES (
                @AccID, @UtilID, @UnitID, @WstID, @ZipW,
                @B1, @B2, @B3, @B4, @B5, @R2,
                @EndDate, @EMoID, @MoCt
                )";

            using (IDbConnection db = new SqlConnection(_jitWebData3ConnectionString))
            {
                rowsAffected = db.Execute(sql, new
                {
                    normalParams.AccID,
                    normalParams.UtilID,
                    normalParams.UnitID,
                    normalParams.WstID,
                    normalParams.ZipW,
                    normalParams.B1,
                    normalParams.B2,
                    normalParams.B3,
                    normalParams.B4,
                    normalParams.B5,
                    normalParams.R2,
                    normalParams.EndDate,
                    normalParams.EMoID,
                    normalParams.MoCt
                });
            }

            return (rowsAffected == 1);
        }

        public bool UpdateWthNormalParams(WthNormalParams normalParams)
        {
            int rowsAffected = 0;
            string sql = @"update WthNormalParams set 
                            B1 = @B1, 
                            B2 = @B2, 
                            B3 = @B3, 
                            B4 = @B4, 
                            B5 = @B5, 
                            R2 = @R2, 
                            EndDate = @EndDate, 
                            EMoID = @EMoID
                        where 
                            AccID = @AccID and 
                            UtilID = @UtilID and 
                            UnitID = @UnitID";

            using (IDbConnection db = new SqlConnection(_jitWebData3ConnectionString))
            {
                rowsAffected = db.Execute(sql, new
                {
                    normalParams.B1,
                    normalParams.B2,
                    normalParams.B3,
                    normalParams.B4,
                    normalParams.B5,
                    normalParams.R2,
                    normalParams.EndDate,
                    normalParams.EMoID,
                    normalParams.AccID,
                    normalParams.UtilID,
                    normalParams.UnitID
                });
            }

            return (rowsAffected == 1);
        }
    }
}
