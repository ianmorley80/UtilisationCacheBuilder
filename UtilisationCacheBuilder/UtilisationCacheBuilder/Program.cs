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
        private enum TimeWindow { Hour,Day,Weekday,Month} 
        private enum Area { Container, Floor, Building }

        private static List<Metric> Metrics;
        private static DateTime CurrentTimeUTC;
        private static DateTime CurrentTimeWorldMax;
        private static DateTime HourlyCacheLastUpdatedUTC;
        private static DateTime MonthlyCacheLastUpdatedUTC;

        private const int HOURLY_CACHE_CONFIGURATIONID = 12008;
        private const int MONTHLY_CACHE_CONFIGURATIONID = 12009;


        private enum CachePeriod { Hourly, Monthly}

        static void Main(string[] args)
        {
            Initialize();

            GenerateHourlyCache();

            return; 

            GenerateAggregatedTimeCache(TimeWindow.Day);
            GenerateAggregatedTimeCache(TimeWindow.Weekday);
            SetCacheLastUpdated(CachePeriod.Hourly);

            // Only generate monthly if the month has changed. 
            if (CurrentTimeWorldMax.Month > MonthlyCacheLastUpdatedUTC.Month || CurrentTimeWorldMax.Year > MonthlyCacheLastUpdatedUTC.Year)
            {
                GenerateAggregatedTimeCache(TimeWindow.Month);
                SetCacheLastUpdated(CachePeriod.Monthly);
            }

        }

        private static void SetCacheLastUpdated(CachePeriod cachePeriod)
        {
            int configurationID = 0;
            switch (cachePeriod)
            {
                case CachePeriod.Hourly:
                    configurationID = HOURLY_CACHE_CONFIGURATIONID;
                    break;
                case CachePeriod.Monthly:
                    configurationID = MONTHLY_CACHE_CONFIGURATIONID;
                    break;
            }
            // Update generateCacheFrom in database
            using (SqlConnection sqlConn = new SqlConnection(ConnectionString))
            {
                var checkStartDateSQL = "UPDATE [Configuration] SET [Value]='" + CurrentTimeUTC.ToString("yyyy/MM/dd HH:mm") + "' WHERE ConfigurationID=" + configurationID;
                SqlCommand cmd = new SqlCommand(checkStartDateSQL, sqlConn);
                cmd.Connection.Open();
                cmd.ExecuteNonQuery();
                sqlConn.Close();
            }
        }

        #region Cache Generation

        private static void Initialize()
        {
            // Initialization
            CurrentTimeUTC = DateTime.UtcNow;
            CurrentTimeUTC.AddMinutes(CurrentTimeUTC.Minute * -1); // remove minute component. 
            CurrentTimeWorldMax = CurrentTimeUTC.AddHours(14); // the maximum time in the world right now is UTC+14

            using (SqlConnection sqlConn = new SqlConnection(ConnectionString))
            {
                var checkStartDateSQL = String.Format("SELECT ConfigurationID,Value FROM [Configuration] WHERE ConfigurationID IN ({0},{1})",HOURLY_CACHE_CONFIGURATIONID,MONTHLY_CACHE_CONFIGURATIONID);
                SqlCommand cmd = new SqlCommand(checkStartDateSQL, sqlConn);
                cmd.Connection.Open();
                using (var sqlReader = cmd.ExecuteReader())
                {
                    while (sqlReader.Read())
                    {
                        var id = sqlReader.GetInt32(0);
                        var result = sqlReader.GetString(1);

                        switch (id)
                        {
                            case HOURLY_CACHE_CONFIGURATIONID:
                                DateTime.TryParse(result, out HourlyCacheLastUpdatedUTC);
                                break;
                            case MONTHLY_CACHE_CONFIGURATIONID:
                                DateTime.TryParse(result, out MonthlyCacheLastUpdatedUTC);
                                break;
                        }
                    }
                }
                cmd.Connection.Close();
            }

            HourlyCacheLastUpdatedUTC = CurrentTimeUTC.AddMonths(-3).AddDays(CurrentTimeUTC.Day * -1).AddHours(CurrentTimeUTC.Hour * -1).AddMinutes(CurrentTimeUTC.Minute * -1); // by default go back 3 months. 
            MonthlyCacheLastUpdatedUTC = HourlyCacheLastUpdatedUTC;

            // create collection of Metrics that we're going to calculate. 
            Metrics = new List<Metric>();
            Metrics.Add(new Metric("Utilisation Start Time", "UST", typeof(DateTime), Metric.SqlType.Smalldatetime, true, Metric.Function.Min, Metric.Function.Min));
            Metrics.Add(new Metric("Utilisation End Time", "UET", typeof(DateTime), Metric.SqlType.Smalldatetime, true, Metric.Function.Max, Metric.Function.Max));
            Metrics.Add(new Metric("Utilised", "UTI", typeof(bool), Metric.SqlType.Bit, false, Metric.Function.Or, Metric.Function.Or));
            Metrics.Add(new Metric("Minutes Utilised", "MIU", typeof(int), Metric.SqlType.Int, false, Metric.Function.Sum, Metric.Function.Sum));
            Metrics.Add(new Metric("Workpoints Utilised", "WPU", typeof(int), Metric.SqlType.Smallint, false, Metric.Function.Max, Metric.Function.Sum));
            Metrics.Add(new Metric("Collaboration Spaces Utilised", "CSU", typeof(int), Metric.SqlType.Smallint, false, Metric.Function.Max, Metric.Function.Sum));
            Metrics.Add(new Metric("Occupancy Minutes", "OCM", typeof(int), Metric.SqlType.Int, false, Metric.Function.Sum, Metric.Function.Sum));
            Metrics.Add(new Metric("Peak Occupancy", "PKO", typeof(int), Metric.SqlType.Smallint, false, Metric.Function.CountDistinct, Metric.Function.CountDistinct));
            Metrics.Add(new Metric("Attendance", "ATT", typeof(int), Metric.SqlType.Smallint, false, Metric.Function.CountDistinct, Metric.Function.CountDistinct));
            Metrics.Add(new Metric("Workpoints", "WPS", typeof(int), Metric.SqlType.Smallint, false, Metric.Function.Max, Metric.Function.Sum));
            Metrics.Add(new Metric("Collaboration Spaces", "CLS", typeof(int), Metric.SqlType.Smallint, false, Metric.Function.Max, Metric.Function.Sum));
            Metrics.Add(new Metric("Peak Workpoints Utilised", "PWU", typeof(int), Metric.SqlType.Smallint, false, Metric.Function.None, Metric.Function.Max));
            Metrics.Add(new Metric("Average Workpoints Utilised", "AWU", typeof(int), Metric.SqlType.Smallint, false, Metric.Function.None, Metric.Function.Avg));
            Metrics.Add(new Metric("Peak Collaboration Spaces Utilised", "PCU", typeof(int), Metric.SqlType.Smallint, false, Metric.Function.None, Metric.Function.Max));
            Metrics.Add(new Metric("Average Collaboration Spaces Utilised", "ACU", typeof(int), Metric.SqlType.Smallint, false, Metric.Function.None, Metric.Function.Avg));

            // Create Database Tables to store cached data (if they don't already exist)
            CreateUtilisationCacheTable(Area.Container, TimeWindow.Hour);
            CreateUtilisationCacheTable(Area.Container, TimeWindow.Day);
            CreateUtilisationCacheTable(Area.Container, TimeWindow.Weekday);
            CreateUtilisationCacheTable(Area.Container, TimeWindow.Month);

            CreateUtilisationCacheTable(Area.Floor, TimeWindow.Hour);
            CreateUtilisationCacheTable(Area.Floor, TimeWindow.Day);
            CreateUtilisationCacheTable(Area.Floor, TimeWindow.Weekday);
            CreateUtilisationCacheTable(Area.Floor, TimeWindow.Month);

            CreateUtilisationCacheTable(Area.Building, TimeWindow.Hour);
            CreateUtilisationCacheTable(Area.Building, TimeWindow.Day);
            CreateUtilisationCacheTable(Area.Building, TimeWindow.Weekday);
            CreateUtilisationCacheTable(Area.Building, TimeWindow.Month);

        }

        private static void GenerateHourlyCache()
        {
            // Hourly Cache is updated for every hour. 

            // To save on memory, process one UTC day's worth of data at a time. 
            for (int dayCount = 0; dayCount <= CurrentTimeUTC.Date.Subtract(HourlyCacheLastUpdatedUTC.Date).TotalDays; dayCount++)
            {
                //DateTime startDate = generateCacheFrom.AddDays(dayCount);

                // Get ContainerOccupancy Data To Process
                DateTime processDate = HourlyCacheLastUpdatedUTC.AddDays(dayCount);
                DateTime maxDate = processDate.AddDays(1);
                if (maxDate > CurrentTimeUTC)
                    maxDate = CurrentTimeUTC;
                var table = GetContainerOccupancyData(processDate, maxDate);

                // Add Local Timezones & Helper Columns 
                AddLocalTimezonesAndHelperDates(table);
                AddMetricColumnsToTable(table, 0, 23);

                Console.WriteLine("Processing Hourly Data for: " + processDate.ToShortDateString() + " with " + table.Count + "Rows");

                DataSet1.ContainerOccupancyRow row;
                for (int i = 0; i < table.Rows.Count; i++)
                {
                    if (i % 1000 == 0)
                        Console.WriteLine("Processed " + i + " rows");

                    row = (DataSet1.ContainerOccupancyRow)table.Rows[i];
                    row.StartDate = new DateTime(row.ActivityStartTimeLocal.Year, row.ActivityStartTimeLocal.Month, row.ActivityStartTimeLocal.Day);

                    for (int hour = 0; hour <= 23; hour++)
                    {
                        //Utilisation Start Time
                        DateTime? utilisationStart = null;
                        if ((row.ActivityStartsPreviousDay || (row.ActivityStartedToday && row.ActivityStartTimeLocal.Hour < hour)) &&
                            (row.UtilisationEndsFutureDay || (row.ActivityStartedToday && row.UtilisationEndTimeLocal.Hour >= hour)))
                            utilisationStart = new DateTime(row.ActivityStartTimeLocal.Year, row.ActivityStartTimeLocal.Month, row.ActivityStartTimeLocal.Day, hour, 0, 0);
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
                                utilisationEnd = row.StartDate.AddHours(hour + 1);
                            else if (row.UtilisationEndsToday && row.UtilisationEndTimeLocal.Hour == hour)
                                utilisationEnd = row.UtilisationEndTimeLocal;

                            if (utilisationEnd != null)
                                row["UET" + hour] = utilisationEnd;
                        }

                        // MIU - Minutes Utilized
                        int minutesUtilised = 0;
                        if (utilisationStart != null)
                            minutesUtilised = Convert.ToInt32(Math.Ceiling(utilisationEnd.Value.Subtract(utilisationStart.Value).TotalMinutes));
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
                        {
                            row["PKO" + hour] = row.OccupiedByPersonID;
                            row["ATT" + hour] = row.OccupiedByPersonID;
                        }
                        else
                        {
                            row["PKO" + hour] = 0;
                            row["ATT" + hour] = 0;
                        }

                        // Workpoints
                        row["WPS" + hour] = row.Workpoints;
                        row["PWU" + hour] = row.Workpoints;
                        row["AWU" + hour] = row.Workpoints;

                        // Collaboration Spaces
                        row["CLS" + hour] = row.Capacity;
                        row["PCU" + hour] = row.Capacity;
                        row["ACU" + hour] = row.Capacity;

                    }

                }

                if (table.Rows.Count > 0)
                {
                    //var filteredTable = table.AsEnumerable().Where(r => r.Field<DateTime?>("StartDate") != null).CopyToDataTable();

                    var groupedTable = AggregateTable(table, "ContainerID", "StartDate", "SpaceUtilisationPerHour", 0, 23, HourlyCacheLastUpdatedUTC);
                    CommitCache2(HourlyCacheLastUpdatedUTC, groupedTable, 0, 23);

                    groupedTable = AggregateTable(table, "FloorID", "StartDate", "SpaceUtilisationByFloorPerHour", 0, 23, HourlyCacheLastUpdatedUTC);
                    CommitCache2(HourlyCacheLastUpdatedUTC, groupedTable, 0, 23);

                    groupedTable = AggregateTable(table, "BuildingID", "StartDate", "SpaceUtilisationByBuildingPerHour", 0, 23, HourlyCacheLastUpdatedUTC);
                    CommitCache2(HourlyCacheLastUpdatedUTC, groupedTable, 0, 23);

                    groupedTable = null;
                }

                table = null;

            }
        }


        private static void GenerateAggregatedLocationCache(Area locationAggregation, DateTime generateCacheFrom, TimeWindow timeAggregation)
        {
            int minValue = 0;
            int maxValue = 0;
            switch (timeAggregation)
            {
                case (TimeWindow.Hour):
                    minValue = 0;
                    maxValue = 23;
                    break;
                case (TimeWindow.Day):
                    minValue = 1;
                    maxValue = 31;
                    break;
                case (TimeWindow.Weekday):
                    minValue = 1;
                    maxValue = 7;
                    break;
                case (TimeWindow.Month):
                    minValue = 1;
                    maxValue = 12;
                    break;
            }


            StringBuilder sb = new StringBuilder();

            sb.Append("SELECT ").Append(locationAggregation).Append("ID,StartDate,");

            var metrics = Metrics.Where(r => r.TimeAggregateFunction != Metric.Function.CountDistinct).Select(r => r).ToList<Metric>();

            for (int i = 0; i < metrics.Count; i++)
            {

                for (int v = minValue; v <= maxValue; v++)
                {
                    if (metrics[i].LocationAggregateFunction == Metric.Function.Or)
                        sb.Append("CAST(MAX(CAST([").Append(metrics[i].Abbreviation).Append(v.ToString("D2")).Append("] as tinyint)) as bit) as ").Append(metrics[i].Abbreviation).Append(v.ToString("D2"));
                    else
                        sb.Append(metrics[i].LocationAggregateFunction).Append("([").Append(metrics[i].Abbreviation).Append(v.ToString("D2")).Append("]) as ").Append(metrics[i].Abbreviation).Append(v.ToString("D2"));
                    if (v != maxValue)
                        sb.Append(",");
                }
                if (i != metrics.Count - 1)
                    sb.Append(",");
            }

            sb.Append(@" FROM SpaceUtilisationPer").Append(timeAggregation).Append(@" su
                INNER JOIN Container c ON(su.ContainerID = c.ContainerID)
                WHERE StartDate >= '2018-05-30'
                GROUP BY ").Append(locationAggregation).Append("ID, StartDate");

            DataTable table = GetData(sb.ToString());
            table.TableName = "SpaceUtilisationBy" + locationAggregation + "Per" + timeAggregation;
            table.PrimaryKey = new DataColumn[] { table.Columns[locationAggregation + "ID"], table.Columns["StartDate"] };

            DataTable attendanceTable = GetAttendanceData(generateCacheFrom, timeAggregation, locationAggregation);
            table.Merge(attendanceTable);

            CommitCache2(generateCacheFrom, table, minValue, maxValue);

        }

        private static void GenerateAggregatedTimeCache(TimeWindow timeAggregation)
        {
            DateTime generateCacheFrom = HourlyCacheLastUpdatedUTC;
            int minValue = 0;
            int maxValue = 0;
            string groupByDate = "";

            switch (timeAggregation)
            {
                case TimeWindow.Day:
                    generateCacheFrom = HourlyCacheLastUpdatedUTC.AddHours(-36); // Max UTC offset of -12 then another day
                    generateCacheFrom = generateCacheFrom.AddHours(generateCacheFrom.Hour * -1); // start of that day. 
                    minValue = 1;
                    maxValue = 31;
                    groupByDate = "StartOfMonth";
                    break;
                case TimeWindow.Weekday:
                    generateCacheFrom = HourlyCacheLastUpdatedUTC.AddHours(-36); // Max UTC offset of -12 then another day
                    generateCacheFrom = generateCacheFrom.AddHours(generateCacheFrom.Hour * -1); // start of that day. 
                    minValue = 1;
                    maxValue = 7;
                    groupByDate = "StartOfWeek";
                    break;
                case TimeWindow.Month:
                    generateCacheFrom = HourlyCacheLastUpdatedUTC.AddMonths(-1).AddDays(HourlyCacheLastUpdatedUTC.Day).AddHours(HourlyCacheLastUpdatedUTC.Hour).AddMinutes(HourlyCacheLastUpdatedUTC.Minute);
                    minValue = 1;
                    maxValue = 12;
                    groupByDate = "StartOfYear";
                    break;
            }

            StringBuilder sb = new StringBuilder();
            sb.Append(@"SELECT su.ContainerID,StartDate, tz.Name as 'TimeZone',
                DATEFROMPARTS(YEAR(StartDate),1,1) as StartOfYear,
                DATEFROMPARTS(YEAR(StartDate), MONTH(StartDate), 1) as StartofMonth,
                DATEADD(ww, DATEDIFF(ww, 0, StartDate), 0) as StartOfWeek");

            for (int i = 0; i < Metrics.Count; i++)
            {
                AppendMultiColumnAggregate(sb, Metrics[i], 0, 23);
            }

            sb.Append(" FROM [SpaceUtilisationPerHour] su INNER JOIN [Container] c ON (su.ContainerID = c.ContainerID) INNER JOIN [Building] b ON (c.BuildingID = b.BuildingID) LEFT OUTER JOIN tzdb.Zones tz ON (b.TimeZoneID = tz.ID)  WHERE StartDate >='").Append(generateCacheFrom.ToString("yyyy/MM/dd HH:mm")).Append("'");

            Console.WriteLine("Prosessing Aggregate Data for " + timeAggregation + " beginning " + generateCacheFrom);

            var table = GetData(sb.ToString());


            AddMetricColumnsToTable(table, minValue, maxValue);

            PivotTable(table, timeAggregation);

            var aggregatedTable = AggregateTable(table, "ContainerID", groupByDate, "SpaceUtilisationPer" + timeAggregation, minValue, maxValue, generateCacheFrom);

            CommitCache2(generateCacheFrom, aggregatedTable, minValue, maxValue);

            // GenerateLocationCache
            GenerateAggregatedLocationCache(Area.Floor, generateCacheFrom, timeAggregation);
            GenerateAggregatedLocationCache(Area.Building, generateCacheFrom, timeAggregation);


        }

        #endregion

        #region DataTable functions

        private static void RemoveMetricColumns(DataTable table, string metric)
        {
            for (int hour = 0; hour <= 23; hour++)
            {
                table.Columns.Remove(metric + hour);
            }

        }


        private static void PivotTable(DataTable table, TimeWindow aggregation)
        {
            foreach (DataRow row in table.Rows)
            {
                foreach (Metric metric in Metrics)
                    PivotValue(row, metric, aggregation);
            }

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

        private static void AddMetricColumnsToTable(DataTable table, int minValue, int maxValue)
        {
            AddMetricColumnsToTable(table, minValue, maxValue, Metrics);
        }

        private static void AddMetricColumnsToTable(DataTable table, int minValue, int maxValue, List<Metric> metrics)
        {
            foreach (Metric metric in metrics)
                AddMetricColumns(table, metric, minValue, maxValue);
        }

        private static DataTable AggregateTable(DataTable sourceTable, string groupByLocationID, string groupByDate, string newTableName, int minValue, int maxValue, DateTime generateCacheFrom)
        {
            return AggregateTable(sourceTable, groupByLocationID, groupByDate, newTableName, minValue, maxValue, generateCacheFrom, Metrics);
        }

        private static DataTable AggregateTable(DataTable sourceTable, string groupByLocationID, string groupByDate, string newTableName, int minValue, int maxValue, DateTime generateCacheFrom, List<Metric> metrics)
        {
            DataTable newTable = new DataTable();
            newTable.Columns.Add(groupByLocationID, typeof(int));
            newTable.Columns.Add("StartDate", typeof(DateTime));
            newTable.Columns.Add("MinCachedDateLocal", typeof(DateTime));

            AddMetricColumnsToTable(newTable, minValue, maxValue, metrics);

            var groupedTable = sourceTable.AsEnumerable()
                            .GroupBy(r => new { LocationID = r.Field<int>(groupByLocationID), StartDate = r.Field<DateTime>(groupByDate), Timezone = r.Field<string>("Timezone") })
                            .Select(g =>
                            {
                                var newRow = newTable.NewRow();

                                newRow[groupByLocationID] = g.Key.LocationID;
                                newRow["StartDate"] = g.Key.StartDate;
                                var timeZone = DateTimeZoneProviders.Tzdb[g.Key.Timezone];
                                newRow["MinCachedDateLocal"] = Instant.FromDateTimeUtc(DateTime.SpecifyKind(generateCacheFrom, DateTimeKind.Utc)).InZone(timeZone).ToDateTimeUnspecified(); ;

                                for (int i = minValue; i <= maxValue; i++)
                                {
                                    foreach (Metric metric in metrics)
                                    {
                                        if (metric.AllowNulls)
                                        {
                                            if (metric.DataType == typeof(DateTime))
                                            {
                                                if (metric.TimeAggregateFunction == Metric.Function.Min)
                                                {
                                                    DateTime? value = g.Min(r => r.Field<DateTime?>(metric.Abbreviation + i));
                                                    if (value != null)
                                                        newRow[metric.Abbreviation + i] = value;
                                                }
                                                else if (metric.TimeAggregateFunction == Metric.Function.Max)
                                                {
                                                    DateTime? value = g.Max(r => r.Field<DateTime?>(metric.Abbreviation + i));
                                                    if (value != null)
                                                        newRow[metric.Abbreviation + i] = value;
                                                }
                                            }
                                        }
                                        else
                                        {
                                            if (metric.TimeAggregateFunction == Metric.Function.Or)
                                                newRow[metric.Abbreviation + i] = g.Where(r => r.Field<bool>(metric.Abbreviation + i) == true).Count() > 0;
                                            else if (metric.TimeAggregateFunction == Metric.Function.Sum)
                                                newRow[metric.Abbreviation + i] = g.Sum(r => r.Field<int>(metric.Abbreviation + i));
                                            else if (metric.TimeAggregateFunction == Metric.Function.CountDistinct)
                                                newRow[metric.Abbreviation + i] = g.Where(r => r.Field<int>(metric.Abbreviation + i) != 0).Select(r => r.Field<int>(metric.Abbreviation + i)).Distinct().Count();
                                        }
                                    }

                                }

                                return newRow;

                            })
                            .CopyToDataTable();

            groupedTable.TableName = newTableName;
            return groupedTable;

        }

        private static void PivotValue(DataRow row, Metric metric, TimeWindow aggregation)
        {
            DateTime startDate = (DateTime)row["StartDate"];
            if (metric.TimeAggregateFunction == Metric.Function.CountDistinct || metric.TimeAggregateFunction == Metric.Function.None)
                return;
            switch (aggregation)
            {
                case TimeWindow.Day:
                    row[metric.Abbreviation + startDate.Day] = row[metric.Abbreviation];
                    break;
                case TimeWindow.Weekday:
                    row[metric.Abbreviation + GetDayOfWeek(startDate.DayOfWeek)] = row[metric.Abbreviation];
                    break;
                case TimeWindow.Month:
                    row[metric.Abbreviation + startDate.Month] = row[metric.Abbreviation];
                    break;
            }

        }

        private static void AppendMultiColumnAggregate(StringBuilder sb, Metric metric, int minValue, int maxValue)
        {

            if (metric.TimeAggregateFunction == Metric.Function.Or)
            {
                sb.Append(",");
                for (int i = minValue; i <= maxValue; i++)
                {
                    sb.Append(metric.Abbreviation).Append(i.ToString("D2"));
                    if (i != maxValue)
                        sb.Append("|");
                }
                sb.Append(" AS ").Append(metric.Abbreviation).Append(" ");
            }
            else if (metric.TimeAggregateFunction != Metric.Function.CountDistinct && metric.TimeAggregateFunction != Metric.Function.None)
            {
                sb.Append(",(SELECT ").Append(metric.TimeAggregateFunction.ToString()).Append("(").Append(metric.Abbreviation).Append(") FROM (VALUES ");
                for (int i = minValue; i <= maxValue; i++)
                {
                    sb.Append("(").Append(metric.Abbreviation).Append(i.ToString("D2")).Append(")");
                    if (i != maxValue)
                        sb.Append(",");
                }
                sb.Append(") AS x(").Append(metric.Abbreviation).Append(")) AS ").Append(metric.Abbreviation);

            }

        }


        #endregion

        #region Database Functions

        private static DataTable GetData(string sql)
        {
            DataTable table = new DataTable();
            using (SqlConnection sqlConn = new SqlConnection(ConnectionString))
            {
                using (SqlCommand cmd = new SqlCommand(sql, sqlConn))
                {
                    SqlDataAdapter da = new SqlDataAdapter(cmd);
                    da.Fill(table);
                }
            }
            return table;
        }

        private static DataTable GetAttendanceData(DateTime generateCacheFrom, TimeWindow timeAggregation, Area locationAggregation)
        {
            int minValue = 0;
            int maxValue = 0;
            switch (timeAggregation)
            {
                case (TimeWindow.Hour):
                    minValue = 0;
                    maxValue = 23;
                    break;
                case (TimeWindow.Day):
                    minValue = 1;
                    maxValue = 31;
                    break;
                case (TimeWindow.Weekday):
                    minValue = 1;
                    maxValue = 7;
                    break;
                case (TimeWindow.Month):
                    minValue = 1;
                    maxValue = 12;
                    break;
            }


            // Get ContainerOccupancy Data To Process
            DateTime processDate = generateCacheFrom;
            DateTime maxDate = processDate.AddDays(1);
            if (maxDate > CurrentTimeUTC)
                maxDate = CurrentTimeUTC;
            var table = GetContainerOccupancyData(processDate, maxDate);

            AddLocalTimezonesAndHelperDates(table);
            var attMetric = new Metric("Attendance", "ATT", typeof(int), Metric.SqlType.Smallint, false, Metric.Function.CountDistinct, Metric.Function.CountDistinct);
            AddMetricColumns(table, attMetric, minValue, maxValue);

            DataSet1.ContainerOccupancyRow row;
            for (int i = 0; i < table.Rows.Count; i++)
            {
                if (i % 1000 == 0)
                    Console.WriteLine("Processed " + i + " rows");

                row = (DataSet1.ContainerOccupancyRow)table.Rows[i];
                row.StartDate = new DateTime(row.ActivityStartTimeLocal.Year, row.ActivityStartTimeLocal.Month, 1);

                for (int v = minValue; v <= maxValue; v++)
                {
                    bool isUtilised = true;

                    if (isUtilised)
                    {
                        row["ATT" + v] = row.OccupiedByPersonID;
                    }
                    else
                    {
                        row["ATT" + v] = 0;
                    }
                }
            }

            var groupedTable = AggregateTable(table, locationAggregation + "ID", "StartDate", "SpaceUtilisationBy" + locationAggregation + "Per" + timeAggregation, minValue, maxValue, processDate, new List<Metric> { attMetric });

            groupedTable.PrimaryKey = new DataColumn[] { groupedTable.Columns["FloorID"], groupedTable.Columns["StartDate"] };

            return groupedTable;

        }

        private static void CommitCache2(DateTime generateCacheFrom, DataTable table, int minValue, int maxValue)
        {
            string locationID = table.Columns[0].ColumnName;

            Dictionary<int, DateTime> latestStartDateCachedForLocation = new Dictionary<int, DateTime>();
            using (SqlConnection sqlConn = new SqlConnection(ConnectionString))
            {
                var getLatestCachedStartDatePerBuidlingSQL = @"select " + locationID + @",MAX(StartDate) as LatestCachedStartDate
                    FROM[" + table.TableName + @"]
                    GROUP BY " + locationID;

                using (SqlCommand cmd = new SqlCommand(getLatestCachedStartDatePerBuidlingSQL, sqlConn))
                {
                    sqlConn.Open();
                    var sqlReader = cmd.ExecuteReader();
                    while (sqlReader.Read())
                    {
                        latestStartDateCachedForLocation.Add((int)sqlReader[locationID], (DateTime)sqlReader["LatestCachedStartDate"]);
                    }
                    sqlConn.Close();
                }

            }


            var sb = new StringBuilder();

            // Update rows where startDate already exists in cache. 
            DataRow row;
            for (int rowCount = 0; rowCount < table.Rows.Count; rowCount++)
            {
                row = table.Rows[rowCount];

                if (latestStartDateCachedForLocation.ContainsKey((int)row[locationID]) &&
                    (DateTime)row["StartDate"] <= latestStartDateCachedForLocation[(int)row[locationID]]) // update that exist in cache
                {
                    {

                        int minRecord = 0;
                        switch (maxValue)
                        {
                            case 23:
                                minRecord = ((DateTime)row["MinCachedDateLocal"]).Hour;
                                break;
                            case 7:
                                minRecord = GetDayOfWeek(((DateTime)row["MinCachedDateLocal"]).DayOfWeek);
                                break;
                            case 31:
                                minRecord = ((DateTime)row["MinCachedDateLocal"]).Day;
                                break;
                            case 12:
                                minRecord = ((DateTime)row["MinCachedDateLocal"]).Month;
                                break;

                        }

                        if (minRecord != maxValue)
                        {
                            sb.Append("UPDATE [").Append(table.TableName).Append("] SET ");

                            for (int i = minRecord; i < maxValue; i++)
                            {
                                for (int m = 0; m < Metrics.Count; m++)
                                {
                                    sb.Append("[").Append(Metrics[m].Abbreviation).Append(i.ToString("D2")).Append("]=");

                                    if (Metrics[m].SqlDataType == Metric.SqlType.Bit)
                                        sb.Append(((bool)row[Metrics[m].Abbreviation + i]) ? 1 : 0);
                                    else if (Metrics[m].SqlDataType == Metric.SqlType.Smalldatetime)
                                        if (row[Metrics[m].Abbreviation + i] != DBNull.Value)
                                            sb.Append("'").Append(((DateTime)row[Metrics[m].Abbreviation + i]).ToString("yyyy/MM/dd HH:mm")).Append("'");
                                        else
                                            sb.Append("null");
                                    else
                                        sb.Append(row[Metrics[m].Abbreviation + i]);

                                    sb.Append(",");
                                }
                            }

                            sb.Remove(sb.Length - 1, 1);

                            sb.Append(" WHERE [").Append(locationID).Append("]= ").Append(row[locationID]).Append(" AND [StartDate] = '").Append(((DateTime)row["StartDate"]).ToString("yyyy/MM/dd")).Append("';");

                        }
                        row.Delete();
                    }
                }
            }

            table.Columns.Remove("MinCachedDateLocal");


            using (SqlConnection sqlConn = new SqlConnection(ConnectionString))
            {
                sqlConn.Open();

                // update rows where StartDate already exists in cache. 
                if (sb.Length > 0)
                {
                    SqlCommand cmd = new SqlCommand(sb.ToString(), sqlConn);
                    cmd.ExecuteNonQuery();
                }

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

        private static void CreateUtilisationCacheTable(Area area, TimeWindow window)
        {
            var byString = area == Area.Container ? String.Empty : "By" + area.ToString();
            var tableName = String.Format("SpaceUtilisation{0}Per{1}", byString, window.ToString());
            var locationID = String.Format("{0}ID", area);

            var sb = new StringBuilder();

            sb.Append(" IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='").Append(tableName);
            sb.Append("' AND xtype='U') CREATE TABLE [dbo].[").Append(tableName).Append("]([").Append(locationID).Append("] [int] NOT NULL,[StartDate] [smalldatetime] NOT NULL,");

            CreateMetricsColumns(sb, window);

            sb.Append(" CONSTRAINT PK_").Append(tableName).Append("_").Append(locationID).Append("_StartDate PRIMARY KEY CLUSTERED (").Append(locationID).Append(", StartDate))");

            using (SqlConnection sqlConn = new SqlConnection(ConnectionString))
            {
                SqlCommand cmd = new SqlCommand(sb.ToString(), sqlConn);
                cmd.Connection.Open();
                cmd.ExecuteNonQuery();
                cmd.Connection.Close();
            }
        }

        private static void CreateMetricsColumns(StringBuilder sb, TimeWindow window)
        {
            for (int i = 0; i < Metrics.Count; i++)
            {
                CreateColumns(sb, Metrics[i].Abbreviation, Metrics[i].SqlDataType.ToString().ToLower(), Metrics[i].AllowNulls, window);
                if (i != Metrics.Count - 1)
                    sb.Append(",");
            }
        }

        private static void CreateColumns(StringBuilder sb, string metric, string type, bool allowNulls, TimeWindow window)
        {
            int minValue = GetMinValue(window);
            int maxValue = GetMaxValue(window);

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

        private static DataSet1.ContainerOccupancyDataTable GetContainerOccupancyData(DateTime minDateUTC, DateTime maxDateUTC)
        {
            //class variable DataTable, so you can edit it later too!
            var table = new DataSet1.ContainerOccupancyDataTable();

            //in some of your methods:
            using (SqlConnection sqlConn = new SqlConnection(ConnectionString))
            {
                //TODO: Fix Timezones
                //TODO: Add Parameters
                string sqlQuery =
                    @"DECLARE @RegenerateFromLogoutTime datetime = '" + minDateUTC.ToString("yyyy/MM/dd HH:mm") + @"'
                    DECLARE @RegenerateToLoginTime datetime = '" + maxDateUTC.ToString("yyyy/MM/dd HH:mm") + @"'
                    DECLARE @MinUtilisationMins as INT = 5

                     
                    SELECT a.*,
		                    LoginTimeUTC as 'ActivityStartTimeUTC',
		                    LogoutTimeUTC as 'ActivityEndTimeUTC',
		                    IIF( LEAD(LoginTimeUTC) OVER (PARTITION BY ContainerID ORDER BY LoginTimeUTC) < LogoutTimeUTC,
			                    LEAD(LoginTimeUTC) OVER (ORDER BY LoginTimeUTC), LogoutTimeUTC) as 'UtilisationEndTimeUTC',
		                    IIF( LEAD(LoginTimeUTC) OVER (PARTITION BY ContainerID, OccupiedByPersonID ORDER BY LoginTimeUTC) < LogoutTimeUTC,
			                    LEAD(LoginTimeUTC) OVER (ORDER BY LoginTimeUTC), LogoutTimeUTC) as 'OccupancyEndTimeUTC'
                    FROM 
                    (
	                    SELECT 
		                    coi.ContainerID,
		                    coi.FloorID,
		                    LoginTimeUTC,
		                    ISNULL(LogoutTimeUTC,GETUTCDATE()) as 'LogoutTimeUTC',
		                    coi.BuildingID,
		                    UtilisationTypeID as 'ActivityTypeID',
		                    ReferenceValueName as 'ActivityType',
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
		                    (ISNULL(LogoutTimeUTC,GETUTCDATE()) >= @RegenerateFromLogoutTime) -- Only select rows that were logged out since RegenerateFromLogoutTime
		                    AND LoginTimeUTC < @RegenerateToLoginTime
		                    AND DATEDIFF(mi,LoginTime,LogoutTime) > @MinUtilisationMins -- Remove rows below Utilization Threshold
                    ) as a ";

                using (SqlCommand cmd = new SqlCommand(sqlQuery, sqlConn))
                {
                    SqlDataAdapter da = new SqlDataAdapter(cmd);
                    da.Fill(table);
                }
            }

            return table;
        }



        #endregion

        #region Helper Functions

        private static int GetDayOfWeek(DayOfWeek dayOfWeek)
        {
            int dayOfWeekValue = Convert.ToInt32(dayOfWeek);
            return dayOfWeekValue == 0 ? 7 : dayOfWeekValue;
        }

        private static int GetMinValue(TimeWindow window)
        {
            int minValue = 0;
            switch (window)
            {
                case (TimeWindow.Hour):
                    minValue = 0;
                    break;
                case (TimeWindow.Day):
                    minValue = 1;
                    break;
                case (TimeWindow.Weekday):
                    minValue = 1;
                    break;
                case (TimeWindow.Month):
                    minValue = 1;
                    break;
            }
            return minValue;
        }

        private static int GetMaxValue(TimeWindow window)
        {
            int maxValue = 0;
            switch (window)
            {
                case (TimeWindow.Hour):
                    maxValue = 23;
                    break;
                case (TimeWindow.Day):
                    maxValue = 31;
                    break;
                case (TimeWindow.Weekday):
                    maxValue = 7;
                    break;
                case (TimeWindow.Month):
                    maxValue = 12;
                    break;
            }
            return maxValue;
        }

        #endregion

  
    }
}
