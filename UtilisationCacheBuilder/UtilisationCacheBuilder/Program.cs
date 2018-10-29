using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using NodaTime;

namespace UtilisationCacheBuilder
{
    class Program
    {
        private static string ConnectionString = "Server=.;Database=Wayfinder_cba;Trusted_Connection=True";

        private static List<Metric> Metrics;

        static void Main(string[] args)
        {
            DateTime generateCacheFrom = new DateTime(2018, 07, 01);
            DateTime generateCacheTo = new DateTime(2018, 07, 09);

            // create collection of Metrics that we're going to calculate. 
            Metrics = new List<Metric>();
            Metrics.Add(new Metric("Utilisation Start Time", "UST", typeof(DateTime), Metric.SqlType.Smalldatetime, true, Metric.Function.Min));
            Metrics.Add(new Metric("Utilisation End Time", "UET", typeof(DateTime), Metric.SqlType.Smalldatetime, true, Metric.Function.Max));
            Metrics.Add(new Metric("Utilised", "UTI", typeof(bool), Metric.SqlType.Bit, false, Metric.Function.Or));
            Metrics.Add(new Metric("Minutes Utilised", "MIU", typeof(int), Metric.SqlType.Int, false, Metric.Function.Sum));
            Metrics.Add(new Metric("Workpoints Utilised", "WPU", typeof(int), Metric.SqlType.Smallint, false, Metric.Function.Max));
            Metrics.Add(new Metric("Collaboration Spaces Utilised", "CSU", typeof(int), Metric.SqlType.Smallint, false, Metric.Function.Max));
            Metrics.Add(new Metric("Occupancy Minutes", "OCM", typeof(int), Metric.SqlType.Int, false, Metric.Function.Sum));
            Metrics.Add(new Metric("Peak Occupancy", "PKO", typeof(int), Metric.SqlType.Smallint, false, Metric.Function.Max));

            // Create Database Tables to store cached data (if they don't already exist)
            CreateMetricsTable("SpaceUtilisationPerHour", 0, 23);
            CreateMetricsTable("SpaceUtilisationPerDay", 1, 31);
            CreateMetricsTable("SpaceUtilisationPerWeekday", 1, 7);
            CreateMetricsTable("SpaceUtilisationPerMonth", 1, 12);

            GenerateHourlyCache(generateCacheFrom, generateCacheTo);

            //GenerateDailyCache(generateCacheFrom, generateCacheTo);


        }

        private static void GenerateDailyCache(DateTime generateCacheFrom, DateTime generateCacheTo)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append(@"SELECT ContainerID,StartDate,
                DATEFROMPARTS(YEAR(StartDate),1,1) as StartOfYear,
                DATEFROMPARTS(YEAR(StartDate), MONTH(StartDate), 1) as StartofMonth,
                DATEADD(ww, DATEDIFF(ww, 0, StartDate), 0) as StartOfWeek, ");

            for (int i = 0; i < Metrics.Count; i++)
            {
                AppendMultiColumnAggregate(sb, Metrics[i], 0, 23);
                if (i != Metrics.Count - 1)
                    sb.Append(",");
            }


            sb.Append(" FROM [SpaceUtilisationPerHour]");

            Console.WriteLine(sb.ToString());

            DataTable table = new DataTable();
            using (SqlConnection sqlConn = new SqlConnection(ConnectionString))
            {
                using (SqlCommand cmd = new SqlCommand(sb.ToString(), sqlConn))
                {
                    SqlDataAdapter da = new SqlDataAdapter(cmd);
                    da.Fill(table);
                }
            }

            AddMetricColumnsToTable(table, 1, 31);


            foreach (DataRow row in table.Rows)
            {
                foreach (Metric metric in Metrics)
                    PivotValue(row, metric);
            }

            var aggregatedTable = AggregateTable(table, "StartOfMonth", "SpaceUtilisationPerDay", 1, 31);

            CommitCache2(generateCacheFrom, aggregatedTable);

            //aggregatedTable = AggregateTable(table, "StartOfWeek", "SpaceUtilisationPerWeekday", 1, 7);

            //CommitCache2(generateCacheFrom, aggregatedTable);

            //aggregatedTable = AggregateTable(table, "StartOfMonth", "SpaceUtilisationPerMonth", 1, 12);

            //CommitCache2(generateCacheFrom, aggregatedTable);


        }

        private static void AddMetricColumnsToTable(DataTable table, int minValue, int maxValue)
        {
            foreach (Metric metric in Metrics)
                AddMetricColumns(table, metric, minValue, maxValue);
        }

        private static DataTable AggregateTable(DataTable sourceTable, string groupByDate, string newTableName, int minValue, int maxValue)
        {
            DataTable newTable = new DataTable();
            newTable.Columns.Add("ContainerID", typeof(int));
            newTable.Columns.Add("StartDate", typeof(DateTime));

            AddMetricColumnsToTable(newTable, minValue, maxValue);

            var groupedTable = sourceTable.AsEnumerable()
                            .GroupBy(r => new { ContainerID = r.Field<int>("ContainerID"), StartDate = r.Field<DateTime>(groupByDate) })
                            .Select(g =>
                            {
                                var newRow = newTable.NewRow();

                                newRow["ContainerID"] = g.Key.ContainerID;
                                newRow["StartDate"] = g.Key.StartDate;

                                for (int i = minValue; i <= maxValue; i++)
                                {
                                    foreach (Metric metric in Metrics)
                                    {
                                        if (metric.AllowNulls)
                                        {
                                            if (metric.DataType == typeof(DateTime))
                                            {
                                                if (metric.AggregateFunction == Metric.Function.Min)
                                                {
                                                    DateTime? value = g.Min(r => r.Field<DateTime?>(metric.Abbreviation + i));
                                                    if (value != null)
                                                        newRow[metric.Abbreviation + i] = value;
                                                }
                                                else if (metric.AggregateFunction == Metric.Function.Max)
                                                {
                                                    DateTime? value = g.Max(r => r.Field<DateTime?>(metric.Abbreviation + i));
                                                    if (value != null)
                                                        newRow[metric.Abbreviation + i] = value;
                                                }
                                            }
                                        }
                                        else
                                        {
                                            if (metric.AggregateFunction == Metric.Function.Or)
                                                newRow[metric.Abbreviation + i] = g.Where(r => r.Field<bool>(metric.Abbreviation + i) == true).Count() > 0;
                                            else if (metric.AggregateFunction == Metric.Function.Sum)
                                                newRow[metric.Abbreviation + i] = g.Sum(r => r.Field<int>(metric.Abbreviation + i));
                                        }
                                    }

                                }

                                return newRow;

                            })
                            .CopyToDataTable();

            groupedTable.TableName = newTableName;
            return groupedTable;

        }

        private static void PivotValue(DataRow row, Metric metric)
        {
            PivotValue(row, metric.Abbreviation);
        }
        private static void PivotValue(DataRow row, string metric)
        {
            DateTime startDate = (DateTime)row["StartDate"];
            row[metric + startDate.Day] = row[metric];
        }

        private static void AppendMultiColumnAggregate(StringBuilder sb, Metric metric, int minValue, int maxValue)
        {
            if (metric.AggregateFunction == Metric.Function.Or)
            {
                for (int hour = 0; hour <= 23; hour++)
                {
                    sb.Append(metric.Abbreviation).Append(hour.ToString("D2"));
                    if (hour != maxValue)
                        sb.Append("|");
                }
                sb.Append(" AS ").Append(metric.Abbreviation).Append(" ");
            }
            else
            {
                sb.Append("(SELECT ").Append(metric.AggregateFunction.ToString()).Append("(").Append(metric.Abbreviation).Append(") FROM (VALUES ");
                for (int i = minValue; i <= maxValue; i++)
                {
                    sb.Append("(").Append(metric.Abbreviation).Append(i.ToString("D2")).Append(")");
                    if (i != maxValue)
                        sb.Append(",");
                }
                sb.Append(") AS x(").Append(metric.Abbreviation).Append(")) AS ").Append(metric.Abbreviation);

            }

        }

        private static void GenerateHourlyCache(DateTime generateCacheFrom, DateTime generateCacheTo)
        {
            var table = GetContainerOccupancyData(generateCacheFrom);

            AddLocalTimezonesAndHelperDates(table);

            AddMetricColumnsToTable(table, 0, 23);
            AddMetricColumns(table, new Metric("Person ID", "PID", typeof(int), Metric.SqlType.Int, false, Metric.Function.CountDistinct), 0, 23); //PersonID this hour. 

            for (int dayCount = 0; dayCount < generateCacheTo.Date.Subtract(generateCacheFrom.Date).TotalDays; dayCount++)
            {
                DateTime startDate = generateCacheFrom.AddDays(dayCount);

                DataView dv = new DataView(table, "ActivityStartTimeLocal < '" + startDate.AddDays(1).ToString("yyyy/MM/dd") + "' AND ActivityEndTimeLocal >='" + startDate.ToString("yyyy/MM/dd") + "'", "", DataViewRowState.CurrentRows);
                Console.WriteLine("Processing StartDate: " + startDate.ToShortDateString() + " with " + dv.Count + "Rows");

                DataSet1.ContainerOccupancyRow row;
                for (int i = 0; i < dv.Count; i++)
                {
                    if (i % 1000 == 0)
                        Console.WriteLine("Processed " + i + " rows");

                    row = (DataSet1.ContainerOccupancyRow)dv[i].Row;
                    row.StartDate = startDate;

                    for (int hour = 0; hour <= 23; hour++)
                    {
                        //Utilisation Start Time
                        DateTime? utilisationStart = null;
                        if ((row.ActivityStartsPreviousDay || (row.ActivityStartedToday && row.ActivityStartTimeLocal.Hour < hour)) &&
                            (row.UtilisationEndsFutureDay || (row.ActivityStartedToday && row.UtilisationEndTimeLocal.Hour >= hour)))
                            utilisationStart = new DateTime(startDate.Year, startDate.Month, startDate.Day, hour, 0, 0);
                        else if (row.ActivityStartedToday && row.ActivityStartTimeLocal.Hour == hour)
                            utilisationStart = row.ActivityStartTimeLocal;

                        if (utilisationStart != null)
                            row["UST" + hour] = utilisationStart;

                        // UTZ - Utilized
                        bool isUtilised = utilisationStart != null;
                        row["UTI" + hour] = isUtilised;


                        //Utilisation End Time
                        DateTime? utilisationEnd = null;
                        if (utilisationStart != null)
                        {
                            if (row.UtilisationEndsFutureDay || (row.UtilisationEndsToday && row.UtilisationEndTimeLocal.Hour > hour))
                                utilisationEnd = new DateTime(startDate.Year, startDate.Month, startDate.Day, hour, 59, 59);
                            else if (row.UtilisationEndsToday && row.UtilisationEndTimeLocal.Hour == hour)
                                utilisationEnd = row.UtilisationEndTimeLocal;

                            if (utilisationEnd != null)
                                row["UET" + hour] = utilisationEnd;
                        }




                        // MIU - Minutes Utilized
                        int minutesUtilised = 0;
                        if (utilisationStart != null)
                            minutesUtilised = utilisationEnd.Value.Minute - utilisationStart.Value.Minute;
                        if (minutesUtilised > 60)
                            Console.WriteLine("EEEK!!");

                        row["MIU" + hour] = minutesUtilised;

                        // WPU - Workpoints Utilized
                        int workpointsUtilised = 0;
                        if (row.Workpoints > 0 && isUtilised)
                            workpointsUtilised = row.Workpoints;
                        row["WPU" + hour] = workpointsUtilised;

                        // CSU - Collaboration Spaces Utilized
                        int collaborationUtilised = 0;
                        if (row.Capacity > 0 && isUtilised)
                            collaborationUtilised = 1;
                        row["CSU" + hour] = collaborationUtilised;

                        //OCM - Occupant Minutes
                        int occupantMinutes = 0;
                        if (row.ActivityStartedToday && row.ActivityStartTimeLocal.Hour == hour && row.OccupancyEndTimeLocal.Hour == hour && row.OccupancyEndsToday) // Activity Started and Ended this Hour
                            occupantMinutes = row.OccupancyEndTimeLocal.Minute - row.ActivityStartTimeLocal.Minute;
                        else if ((row.ActivityStartsPreviousDay || row.ActivityStartTimeLocal.Hour < hour) && row.OccupancyEndTimeLocal.Hour == hour) // Activity Started in the Past and Ended this Hour
                            occupantMinutes = row.OccupancyEndTimeLocal.Minute;
                        else if ((row.ActivityStartsPreviousDay || row.ActivityStartTimeLocal.Hour < hour) && (row.OccupancyEndTimeLocal.Hour > hour || row.OccupancyEndsFutureDay)) // Activity started in the Past and Ended in a Future Hour
                            occupantMinutes = 60;
                        else if ((row.ActivityStartedToday && row.ActivityStartTimeLocal.Hour == hour) && (row.OccupancyEndTimeLocal.Hour > hour || row.OccupancyEndsFutureDay)) // Activity started this Hour and Ended in a Future Hour
                            occupantMinutes = 60 - row.ActivityStartTimeLocal.Minute;
                        else
                            occupantMinutes = 0; // No Activity this Hour
                        row["OCM" + hour] = occupantMinutes;

                        //PID - PersonID This Hour
                        if (isUtilised)
                            row["PID" + hour] = row.OccupiedByPersonID;
                        else
                            row["PID" + hour] = 0;

                    }

                }

                var filteredTable = table.AsEnumerable().Where(r => r.Field<DateTime?>("StartDate") != null).CopyToDataTable();

                var groupedTable = AggregateTable(filteredTable, "StartDate", "SpaceUtilisationByHour", 0, 23);

                CommitCache(generateCacheFrom, groupedTable);

            }
        }

        private static void RemoveMetricColumns(DataTable table, string metric)
        {
            for (int hour = 0; hour <= 23; hour++)
            {
                table.Columns.Remove(metric + hour);
            }

        }

        private static void CommitCache(DateTime generateCacheFrom, DataTable table)
        {

            bool startDateExistsInCache = false;
            using (SqlConnection sqlConn = new SqlConnection(ConnectionString))
            {
                var checkStartDateInCacheSQL = "SELECT COUNT(*) FROM [SpaceUtilisationPerHour] WHERE StartDate='" + generateCacheFrom.ToString("yyyy/MM/dd") + "'";
                SqlCommand cmd = new SqlCommand(checkStartDateInCacheSQL, sqlConn);
                cmd.Connection.Open();
                startDateExistsInCache = (int)cmd.ExecuteScalar() > 0;
            }

            var sb = new StringBuilder();

            // Delete any cached data > generateCacheFrom date so that we don't have to write UPDATE statements for it. 
            sb.Append("DELETE FROM [SpaceUtilisationPerHour] WHERE StartDate > '").Append(generateCacheFrom.ToString("yyyy/MM/dd")).Append("';");

            // Update rows where startDate already exists in cache. 
            DataRow row;
            for (int rowCount = 0; rowCount < table.Rows.Count; rowCount++)
            {
                row = table.Rows[rowCount];

                if (startDateExistsInCache && (DateTime)row["startDate"] == generateCacheFrom.Date) // update that exist in cache
                {
                    sb.Append("UPDATE [SpaceUtilisationPerHour] SET ");

                    for (int hour = 0; hour < 23; hour++)
                    {
                        sb.Append("[UTI").Append(hour.ToString("D2")).Append("]=").Append(((bool)row["UTI" + hour]) ? 1 : 0).Append(",");
                    }

                    sb.Remove(sb.Length - 1, 1);

                    sb.Append(" WHERE [ContainerID]=").Append(row["ContainerID"]).Append(" AND [StartDate] = '").Append(((DateTime)row["StartDate"]).ToString("yyyy/MM/dd")).Append("';");
                    row.Delete();
                }
            }

            using (SqlConnection sqlConn = new SqlConnection(ConnectionString))
            {
                sqlConn.Open();

                // update rows where StartDate already exists in cache. 
                SqlCommand cmd = new SqlCommand(sb.ToString(), sqlConn);
                cmd.ExecuteNonQuery();

                // bulk insert remaining data
                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConn))
                {
                    bulkCopy.DestinationTableName = "SpaceUtilisationPerHour";

                    try
                    {
                        bulkCopy.BatchSize = 1000;
                        bulkCopy.WriteToServer(table);

                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }

                    Console.WriteLine("Committed " + table.Rows.Count + " Records");
                }

                sqlConn.Close();

            }
        }

        private static void CommitCache2(DateTime generateCacheFrom, DataTable table)
        {

            bool startDateExistsInCache = false;
            using (SqlConnection sqlConn = new SqlConnection(ConnectionString))
            {
                var checkStartDateInCacheSQL = "SELECT COUNT(*) FROM [" + table.TableName + "] WHERE StartDate='" + generateCacheFrom.ToString("yyyy/MM/dd") + "'";
                SqlCommand cmd = new SqlCommand(checkStartDateInCacheSQL, sqlConn);
                cmd.Connection.Open();
                startDateExistsInCache = (int)cmd.ExecuteScalar() > 0;
            }

            var sb = new StringBuilder();

            // Delete any cached data > generateCacheFrom date so that we don't have to write UPDATE statements for it. 
            sb.Append("DELETE FROM [" + table.TableName + "] WHERE StartDate > '").Append(generateCacheFrom.ToString("yyyy/MM/dd")).Append("';");

            // Update rows where startDate already exists in cache. 
            //DataRow row;
            //for (int rowCount = 0; rowCount < table.Rows.Count; rowCount++)
            //{
            //    row = table.Rows[rowCount];

            //    if (startDateExistsInCache && (DateTime)row["startDate"] == generateCacheFrom.Date) // update that exist in cache
            //    {
            //        sb.Append("UPDATE [").Append(table.TableName).Append("] SET ");

            //        for (int hour = 0; hour < 23; hour++)
            //        {
            //            sb.Append("[UTI").Append(hour.ToString("D2")).Append("]=").Append(((bool)row["UTI" + hour]) ? 1 : 0).Append(",");
            //        }

            //        sb.Remove(sb.Length - 1, 1);

            //        sb.Append(" WHERE [ContainerID]=").Append(row["ContainerID"]).Append(" AND [StartDate] = '").Append(((DateTime)row["StartDate"]).ToString("yyyy/MM/dd")).Append("';");
            //        row.Delete();
            //    }
            //}


            using (SqlConnection sqlConn = new SqlConnection(ConnectionString))
            {
                sqlConn.Open();

                // update rows where StartDate already exists in cache. 
                //SqlCommand cmd = new SqlCommand(sb.ToString(), sqlConn);
                //cmd.ExecuteNonQuery();

                // bulk insert remaining data
                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConn))
                {
                    bulkCopy.DestinationTableName = table.TableName;

                    try
                    {
                        bulkCopy.BatchSize = 1000;
                        bulkCopy.WriteToServer(table);

                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }

                    Console.WriteLine("Committed " + table.Rows.Count + " Records");
                }

                sqlConn.Close();

            }
        }



        private static void CreateMetricsTable(string tableName, int minValue, int maxValue)
        {
            var sb = new StringBuilder();

            sb.Append(
                @" IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='" + tableName + @"' AND xtype='U')
                
                    CREATE TABLE [dbo].[" + tableName + @"]( 
                    [ContainerID] [int] NOT NULL,
                    [StartDate] [smalldatetime] NOT NULL,"
                );


            CreateMetricsColumns(sb, minValue, maxValue);

            sb.Append(" CONSTRAINT PK_ContainerID_StartDate PRIMARY KEY CLUSTERED (ContainerID,StartDate))");

            using (SqlConnection sqlConn = new SqlConnection(ConnectionString))
            {
                SqlCommand cmd = new SqlCommand(sb.ToString(), sqlConn);
                cmd.Connection.Open();
                cmd.ExecuteNonQuery();
                cmd.Connection.Close();
            }


        }

        private static void CreateMetricsColumns(StringBuilder sb, int minValue, int maxValue)
        {
            for (int i = 0; i < Metrics.Count; i++)
            {
                CreateColumns(sb, Metrics[i].Abbreviation, Metrics[i].SqlDataType.ToString().ToLower(), Metrics[i].AllowNulls, minValue, maxValue);
                if (i != Metrics.Count - 1)
                    sb.Append(",");
            }
        }

        private static void CreateColumns(StringBuilder sb, string metric, string type, bool allowNulls, int minValue, int maxValue)
        {
            for (int i = minValue; i <= maxValue; i++)
            {
                sb.Append("[").Append(metric).Append(i.ToString("D2")).Append("] [").Append(type).Append("]");
                if (!allowNulls)
                    sb.Append(" NOT NULL");

                if (i != maxValue)
                    sb.Append(",");
            }

        }

        private static void CreateInsertColumns(StringBuilder sb, string metric)
        {
            for (int hour = 0; hour < 23; hour++)
            {
                sb.Append("[").Append(metric).Append(hour.ToString("D2")).Append("],");
            }
        }

        private static void CreateInsertValues(StringBuilder sb, string metric, DataRow row)
        {
            for (int hour = 0; hour < 23; hour++)
            {
                if (metric == "UTI")
                    sb.Append(((bool)row[metric + hour]) ? 1 : 0);
                else if (metric == "UST" || metric == "UET")
                {
                    if (row[metric + hour] != DBNull.Value)
                        sb.Append("'").Append(((DateTime)row[metric + hour]).ToString("yyyy/MM/dd HH:mm")).Append("'");
                    else
                        sb.Append("null");
                }
                else
                    sb.Append(row[metric + hour]);
                sb.Append(",");
            }

        }



        private static DataSet1.ContainerOccupancyDataTable GetContainerOccupancyData(DateTime generateCacheFromTime)
        {
            //class variable DataTable, so you can edit it later too!
            var table = new DataSet1.ContainerOccupancyDataTable();

            //in some of your methods:
            using (SqlConnection sqlConn = new SqlConnection(ConnectionString))
            {
                //TODO: Fix Timezones
                //TODO: Add Parameters
                string sqlQuery =
                    @"DECLARE @RegenerateFromLogoutTime datetime = '" + generateCacheFromTime.ToString("yyyy/MM/dd HH:mm") + @"'
                    DECLARE @MinUtilisationMins as INT = 5

                    SELECT 
	                    coi.ContainerID,
	                    coi.FloorID,
	                    coi.BuildingID,
	                    CONVERT(DateTime,LoginTime AT TIME ZONE 'AUS Eastern Standard Time' AT TIME ZONE 'UTC') as 'ActivityStartTimeUTC',
	                    CONVERT(DateTime,LogoutTime AT TIME ZONE 'AUS Eastern Standard Time' AT TIME ZONE 'UTC') as 'ActivityEndTimeUTC',
	                    UtilisationTypeID as 'ActivityTypeID',
	                    ReferenceValueName as 'ActivityType',
	                    CONVERT(DateTime,IIF( LEAD(LoginTime) OVER (PARTITION BY coi.ContainerID ORDER BY LoginTime) < LogoutTime,
		                    LEAD(LoginTime) OVER (ORDER BY LoginTime), LogoutTime) AT TIME ZONE 'AUS Eastern Standard Time' AT TIME ZONE 'UTC') as 'UtilisationEndTimeUTC',
	                    CONVERT(DateTime,IIF( LEAD(LoginTime) OVER (PARTITION BY coi.ContainerID, coi.OccupiedByPersonID ORDER BY LoginTime) < LogoutTime,
		                    LEAD(LoginTime) OVER (ORDER BY LoginTime), LogoutTime) AT TIME ZONE 'AUS Eastern Standard Time' AT TIME ZONE 'UTC') as 'OccupancyEndTimeUTC',
	                    OccupiedByPersonID,
	                    up.UtilisationProviderID,
	                    UtilisationProviderName,
	                    UtilisationTypeID,
	                    Workpoints,
	                    Capacity,
	                    tz.Name as 'TimeZone'
                    FROM ContainerOccupancyIntermediate coi
	                    INNER JOIN Container c ON (coi.ContainerID = c.ContainerID)
	                    INNER JOIN Building b ON (c.BuildingID = b.BuildingID)
	                    INNER JOIN UtilisationSource us ON (coi.UtilisationSourceID = us.UtilisationSourceID)
	                    INNER JOIN UtilisationProvider up ON (us.UtilisationProviderID = up.UtilisationProviderID)
	                    INNER JOIN ReferenceValue rv ON (coi.UtilisationTypeID = rv.ReferenceValueID)
	                    LEFT OUTER JOIN tzdb.Zones tz ON (b.TimeZoneID = tz.ID)
                    WHERE 
	                    (LogoutTime >= @RegenerateFromLogoutTime OR LogoutTime IS NULL) -- Only select rows that were logged out since RegenerateFromLogoutTime
		                     AND DATEDIFF(mi,LoginTime,LogoutTime) > @MinUtilisationMins -- Remove rows below Utilization Threshold";

                using (SqlCommand cmd = new SqlCommand(sqlQuery, sqlConn))
                {
                    SqlDataAdapter da = new SqlDataAdapter(cmd);
                    da.Fill(table);
                }
            }

            return table;
        }

        private static void AddLocalTimezonesAndHelperDates(DataSet1.ContainerOccupancyDataTable table)
        {
            foreach (DataSet1.ContainerOccupancyRow row in table.Rows)
            {
                var timezone = DateTimeZoneProviders.Tzdb[row.TimeZone];
                row.ActivityStartTimeLocal = Instant.FromDateTimeUtc(DateTime.SpecifyKind(row.ActivityStartTimeUTC, DateTimeKind.Utc)).InZone(timezone).ToDateTimeUnspecified();

                if (row["ActivityEndTimeUTC"] == DBNull.Value)
                    row.ActivityEndTimeUTC = DateTime.UtcNow;
                if (row["UtilisationEndTimeUTC"] == DBNull.Value)
                    row.UtilisationEndTimeUTC = DateTime.UtcNow;
                if (row["OccupancyEndTimeUTC"] == DBNull.Value)
                    row.OccupancyEndTimeUTC = DateTime.Now;
                row.ActivityEndTimeLocal = Instant.FromDateTimeUtc(DateTime.SpecifyKind(row.ActivityEndTimeUTC, DateTimeKind.Utc)).InZone(timezone).ToDateTimeUnspecified();
                row.UtilisationEndTimeLocal = Instant.FromDateTimeUtc(DateTime.SpecifyKind(row.UtilisationEndTimeUTC, DateTimeKind.Utc)).InZone(timezone).ToDateTimeUnspecified();
                row.OccupancyEndTimeLocal = Instant.FromDateTimeUtc(DateTime.SpecifyKind(row.OccupancyEndTimeUTC, DateTimeKind.Utc)).InZone(timezone).ToDateTimeUnspecified();
            }

        }

        private static void AddMetricColumns(DataTable table, Metric metric, int minValue, int maxValue)
        {
            for (int i = minValue; i <= maxValue; i++)
            {
                DataColumn dc = new DataColumn(metric.Abbreviation + i, metric.DataType);
                if (metric.DataType == typeof(int)) // initialize integer metrics to 0. 
                    dc.DefaultValue = 0;
                if (metric.DataType == typeof(bool)) // initialize bool metrics to false.
                    dc.DefaultValue = false;
                table.Columns.Add(dc);
            }

        }
    }
}
