﻿using Dapper;
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
                //allZips = db.Query<string>(@"select distinct b.Zip from Buildings as b
                //                                join Accounts as a on b.BldID = a.BldID
                //                                join WthNormalParams as w on a.AccID = w.AccID").AsList();

                allZips = db.Query<string>(@"select distinct ZipW from WthNormalParams").AsList();
            }

            return allZips;
        }

        public DateTime GetEarliestDateNeededForZipWeather(int MoID, string ZipCode)
        {
            DateTime earliestDate = new DateTime(2015, 1, 1);

      //      string sql = @"select top(1)r.DateStart from Readings r
      //                  join WthNormalParams wnp
      //                  on wnp.AccID = r.AccID and wnp.UtilID = r.UtilID and wnp.UnitID = r.UnitID
						//join Accounts a on wnp.AccID = a.AccID
						//join Buildings b on a.BldID = b.BldID
      //                  where r.MoID >= @MoID
      //                  and b.Zip = @ZipCode
      //                  and r.DateStart is not null
      //                  order by r.DateStart";

            string sql = @"select top(1)r.DateStart from Readings r
                        join WthNormalParams wnp
                        on wnp.AccID = r.AccID and wnp.UtilID = r.UtilID and wnp.UnitID = r.UnitID
                        where r.MoID >= @MoID
                        and wnp.ZipW = @ZipCode
                        and r.DateStart is not null
                        order by r.DateStart";

            using (IDbConnection db = new SqlConnection(_jitWebData3ConnectionString))
            {
                earliestDate = db.Query<DateTime>(sql, new { MoID, ZipCode }).FirstOrDefault();
            }

            return earliestDate;
        }

        public int InsertWeatherData(WeatherData weatherData)
        {
            string sql = @"INSERT INTO [WeatherData] ([StationId], [RDate], [HighTmp], [LowTmp], [AvgTmp]) 
                            VALUES (@StationId, @RDate, @HighTmp, @LowTmp, @AvgTmp);
                            SELECT CAST(SCOPE_IDENTITY() as int)";

            using (IDbConnection db = new SqlConnection(_jitWeatherConnectionString))
            {
                int WdID = db.Query<int>(sql, new
                    {
                        weatherData.StationID,
                        weatherData.ZipCode,
                        weatherData.RDate,
                        weatherData.HighTmp,
                        weatherData.LowTmp,
                        weatherData.AvgTmp
                    }
                ).Single();

                return WdID;
            }
        }

        public bool InsertZipDate(string Zip, string RDate, int WdID)
        {
            string sql = @"insert into ZipDates (Zip, RDate, WdID) values (@Zip, @RDate, @WdID);";

            using (IDbConnection db = new SqlConnection(_jitWeatherConnectionString))
            {
                int rowsAffected = db.Execute(sql, new { Zip, RDate, WdID });

                return rowsAffected == 1;
            }
        }


        public bool GetWeatherDataForZipDateExists(string ZipCode, string RDate)
        {
            bool exists;

            string sql = @"select count(1) from ZipDates join WeatherData on WeatherData.WdID = ZipDates.WdID 
                            where ZipDates.Zip = @ZipCode and WeatherData.RDate = @RDate";

            //DateTime RDateStr = Convert.ToDateTime(RDate.ToShortDateString());

            using (IDbConnection db = new SqlConnection(_jitWeatherConnectionString))
            {
                exists = db.ExecuteScalar<bool>(sql, new { ZipCode, RDate });
            }

            return exists;
        }

        public bool GetWeatherDataForStationAndDateExists(string StationID, string RDate)
        {
            bool exists;

            string sql = @"select count(1) from WeatherData where StationID = @StationID and RDate = @RDate";

            //DateTime RDateStr = Convert.ToDateTime(RDate.ToShortDateString());

            using (IDbConnection db = new SqlConnection(_jitWeatherConnectionString))
            {
                exists = db.ExecuteScalar<bool>(sql, new { StationID, RDate });
            }

            return exists;
        }

        public int GetWdIDFromWeatherData(string StationID, string RDate)
        {
            string sql = @"select WdID from WeatherData where StationID = @StationID and RDate = @RDate";

            using (IDbConnection db = new SqlConnection(_jitWeatherConnectionString))
            {
                return db.Query<int>(sql, new { StationID, RDate }).Single<int>();
            }
        }

        public int GetZipDateRowCount()
        {
            string sql = @"select count(ZipDates.WdID) from ZipDates left outer join WeatherData on WeatherData.WdID = ZipDates.WdID";

            using (IDbConnection db = new SqlConnection(_jitWeatherConnectionString))
            {
                return db.ExecuteScalar<int>(sql);
            }
        }

        public int GetZipDateRowCountByZip(string ZipCode)
        {
            string sql = @"select count(ZipDates.WdID) from ZipDates left outer join WeatherData on WeatherData.WdID = ZipDates.WdID where ZipDates.Zip = @ZipCode";

            using (IDbConnection db = new SqlConnection(_jitWeatherConnectionString))
            {
                return db.ExecuteScalar<int>(sql, new { ZipCode });
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

        public List<ReadingsQueryResult> GetReadings(int MoID)
        {
            string DateEnd = GetMostRecentWeatherDataDate().AddDays(1).ToShortDateString();
            var data = new List<ReadingsQueryResult>();

            string Sql = @"select r.RdngID, b.Zip, r.DateStart, r.DateEnd, r.Days, r.Units, 
                                  wnp.AccID, wnp.UtilID, wnp.UnitID, wnp.B1, wnp.B2, wnp.B3, wnp.B4, wnp.B5, wnp.R2 
                            from Readings r 
                            join WthNormalParams wnp on wnp.AccID = r.AccID
                                                    and wnp.UtilID = r.UtilID
                                                    and wnp.UnitID = r.UnitID
                            join Accounts a on a.AccID = r.AccID
                            join Buildings b on b.BldID = a.BldID
                            where not exists 
                                (select weu.RdngID from WthExpUsage weu
                                    where weu.RdngID = r.RdngID)
                            and r.MoID >= @MoID
                            and r.DateEnd <= @DateEnd
                            and wnp.R2 is not null 
                            order by DateStart asc";

            using (IDbConnection db = new SqlConnection(_jitWebData3ConnectionString))
            {
                return db.Query<ReadingsQueryResult>(Sql, new { MoID, DateEnd }).AsList();
            }
        }

        public List<ReadingsQueryResult> GetReadingsForExpUsageUpdate(int MoID, WthNormalParams normalParams)
        {
            string DateEnd = GetMostRecentWeatherDataDate().AddDays(1).ToShortDateString();
            var data = new List<ReadingsQueryResult>();

            string Sql = @"select r.RdngID, b.Zip, r.DateStart, r.DateEnd, r.Days, r.Units,   
                                  wnp.AccID, wnp.UtilID, wnp.UnitID, wnp.B1, wnp.B2, wnp.B3, wnp.B4, wnp.B5
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
                            r.MoID >= @MoID and
                            r.DateEnd <= @DateEnd
                            and wnp.R2 is not null 
                            order by DateStart asc";

            using (IDbConnection db = new SqlConnection(_jitWebData3ConnectionString))
            {
                return db.Query<ReadingsQueryResult>(Sql, new {
                    normalParams.AccID, normalParams.UtilID, normalParams.UnitID, MoID, DateEnd
                }).AsList();
            }
        }

        public int GetExpectedWthExpUsageRowCount(int MoID)
        {
            string DateEnd = GetMostRecentWeatherDataDate().AddDays(1).ToShortDateString();

            string sql = @"select count(r.RdngID) 
                           from Readings r 
                           join WthNormalParams wnp on wnp.AccID = r.AccID
                                                    and wnp.UtilID = r.UtilID
                                                    and wnp.UnitID = r.UnitID
                           join Accounts a on a.AccID = r.AccID
                           join Buildings b on b.BldID = a.BldID
                           where  r.MoID >= @MoID
                              and r.DateEnd <= @DateEnd
                              and wnp.R2 is not null";

            using (IDbConnection db = new SqlConnection(_jitWebData3ConnectionString))
            {
                return db.ExecuteScalar<int>(sql, new { MoID, DateEnd });
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

            string Sql = @"select wd.WdID, StationID, (RTRIM(zd.Zip)) as ZipCode, wd.RDate, HighTmp, LowTmp, AvgTmp from WeatherData wd 
	                        join ZipDates zd on wd.WdID = zd.WdID
                             WHERE Zip = @ZipCode  AND zd.RDate >= @DateStart and zd.RDate < @DateEnd";

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

        public void ClearWthNormalParams()
        {
            var oldPs = new List<WthNormalParams>();

            using (IDbConnection db = new SqlConnection(_jitWebData3ConnectionString))
            {
                oldPs =  db.Query<WthNormalParams>("Select * from WthNormalParams").AsList();
            }

            foreach(WthNormalParams p in oldPs)
            {
                string sql = "Update WthNormalParams Set B1 = null, B2 = null, B3 = null, B4 = null, B5 = null, R2 = null " +
                    "where AccID = @AccID and UtilID = @UtilID and UnitID = @UnitID";

                using (IDbConnection db = new SqlConnection(_jitWebData3ConnectionString))
                {
                    db.Execute(sql, new { p.AccID, p.UtilID, p.UnitID });
                }
            }
        }
    }
}
