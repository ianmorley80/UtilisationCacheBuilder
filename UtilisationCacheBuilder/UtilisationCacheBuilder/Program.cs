using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using NodaTime;
using System.Diagnostics;

namespace UtilisationCacheBuilder
{
    class Program
    {
        private static string ConnectionString = "Server=.;Database=Wayfinder_cba;Trusted_Connection=True;;Connection Timeout=300";
        private enum TimeWindow { Hour,Day,Weekday,Month} 
        private enum Area { Container, Floor, Building }
        private enum AggregationType { Hour, Time, Area }

        private static List<Metric> Metrics;
        private static DateTime CurrentTimeUTC;
        private static DateTime CurrentTimeWorldMax;
        private static DateTime HourlyCacheLastUpdatedUTC;
        private static DateTime MonthlyCacheLastUpdatedUTC;

        private const int HOURLY_CACHE_CONFIGURATIONID = 12008;
        private const int MONTHLY_CACHE_CONFIGURATIONID = 12009;

        private const int IGNORE_UTILIZATION_BLIP_LESS_THAN_MINS = 5; 

        private enum CachePeriod { Hourly, Monthly}

        static void Main(string[] args)
        {
            Initialize();

            GenerateCachesForTimeWindow(TimeWindow.Hour);
            GenerateCachesForTimeWindow(TimeWindow.Day);
            GenerateCachesForTimeWindow(TimeWindow.Weekday);
            GenerateCachesForTimeWindow(TimeWindow.Month);

            return; 
            //return; 

            //GenerateCachesForTimeWindow(TimeWindow.Day);

            //GenerateCachesForTimeWindow(TimeWindow.Weekday);

            //SetCacheLastUpdated(CachePeriod.Hourly);

            // Only generate monthly if the month has changed. 
            if (CurrentTimeWorldMax.Month > MonthlyCacheLastUpdatedUTC.Month || CurrentTimeWorldMax.Year > MonthlyCacheLastUpdatedUTC.Year)
            {
                GenerateCachesForTimeWindow(TimeWindow.Month);
                //SetCacheLastUpdated(CachePeriod.Monthly);
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
            CurrentTimeUTC = new DateTime(2018, 07, 03,23,45,0); // DateTime.UtcNow;
            CurrentTimeWorldMax = CurrentTimeUTC.WorldMax(); // the maximum time in the world right now is UTC+14

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

            // By Default we'll regenerate the caches for the last 3 months. 
            if (HourlyCacheLastUpdatedUTC == null)
                HourlyCacheLastUpdatedUTC = CurrentTimeUTC.StartOfMonth().AddMonths(-3);  
            if (MonthlyCacheLastUpdatedUTC == null)
                MonthlyCacheLastUpdatedUTC = HourlyCacheLastUpdatedUTC;

            HourlyCacheLastUpdatedUTC = HourlyCacheLastUpdatedUTC.StartOfHour();

            // create collection of Metrics that we're going to calculate. 
            Metrics = new List<Metric>();
            Metrics.Add(new Metric("Utilisation Start Time", "UST", typeof(DateTime), Metric.SqlType.Smalldatetime, true, Metric.Function.Min));
            Metrics.Add(new Metric("Utilisation End Time", "UET", typeof(DateTime), Metric.SqlType.Smalldatetime, true, Metric.Function.Max));
            Metrics.Add(new Metric("Utilised", "UTI", typeof(bool), Metric.SqlType.Bit, false, Metric.Function.Or));
            Metrics.Add(new Metric("Minutes Utilised", "MIU", typeof(int), Metric.SqlType.Int, false, Metric.Function.Max, Metric.Function.Sum, Metric.Function.Max, Metric.Function.Sum));
            Metrics.Add(new Metric("Workpoints Utilised", "WPU", typeof(int), Metric.SqlType.Smallint, false, Metric.Function.Max, Metric.Function.Max, Metric.Function.Sum, Metric.Function.Sum));
            Metrics.Add(new Metric("Collaboration Spaces Utilised", "CSU", typeof(int), Metric.SqlType.Smallint, false, Metric.Function.Max, Metric.Function.Max, Metric.Function.Sum, Metric.Function.Sum));
            Metrics.Add(new Metric("Occupancy Minutes", "OCM", typeof(int), Metric.SqlType.Int, false, Metric.Function.Sum));
            Metrics.Add(new Metric("Peak Occupancy", "PKO", typeof(int), Metric.SqlType.Smallint, false, Metric.Function.CountDistinct, Metric.Function.Max, Metric.Function.Sum,Metric.Function.Max));
            Metrics.Add(new Metric("Attendance", "ATT", typeof(int), Metric.SqlType.Smallint, false, Metric.Function.CountDistinct));
            Metrics.Add(new Metric("Workpoints", "WPS", typeof(int), Metric.SqlType.Smallint, false, Metric.Function.Max, Metric.Function.Max, Metric.Function.Sum, Metric.Function.Sum));
            Metrics.Add(new Metric("Collaboration Spaces", "CLS", typeof(int), Metric.SqlType.Smallint, false, Metric.Function.Max, Metric.Function.Max, Metric.Function.Sum, Metric.Function.Sum));
            Metrics.Add(new Metric("Peak Workpoints Utilised", "PWU", typeof(int), Metric.SqlType.Smallint, false, Metric.Function.Max, Metric.Function.Max, Metric.Function.Sum,Metric.Function.PeakHourly));
            Metrics.Add(new Metric("Average Workpoints Utilised", "AWU", typeof(int), Metric.SqlType.Smallint, false, Metric.Function.Max, Metric.Function.Max, Metric.Function.Sum, Metric.Function.AvgHourly));
            Metrics.Add(new Metric("Peak Collaboration Spaces Utilised", "PCU", typeof(int), Metric.SqlType.Smallint, false, Metric.Function.Max, Metric.Function.Max, Metric.Function.Sum, Metric.Function.PeakHourly));
            Metrics.Add(new Metric("Average Collaboration Spaces Utilised", "ACU", typeof(int), Metric.SqlType.Smallint, false, Metric.Function.Max, Metric.Function.Max, Metric.Function.Sum, Metric.Function.AvgHourly));

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

        private static void GenerateHourlyCacheForSpaces()
        {
            DateTime startPeriod;
            DateTime endPeriod;
            DateTime? utilisationStart = null;
            DateTime? utilisationEnd = null;
            int minutesUtilised;
            int occupantMinutes = 0;

            Stopwatch stopwatch = new Stopwatch();

            // To save on memory, process one UTC day's worth of data at a time. 
            var totalDays = CurrentTimeUTC.Date.Subtract(HourlyCacheLastUpdatedUTC.Date).TotalDays;
            for (int dayCount = 0; dayCount <= totalDays; dayCount++)
            {
                stopwatch.Restart();

                // Get ContainerOccupancy Data To Process
                DateTime processDate = HourlyCacheLastUpdatedUTC.AddDays(dayCount);
                DateTime maxDate = processDate.AddDays(1);
                if (maxDate > CurrentTimeUTC)
                    maxDate = CurrentTimeUTC;

                var table = GetContainerOccupancyData(processDate, maxDate);

                Console.WriteLine(String.Format("Processing Hourly Data for UTC day beginning {0} with {1} records ",processDate,table.Count));

                // Add Local Timezones & Helper Columns 
                AddLocalTimezonesAndHelperDates(table);
                AddMetricColumnsToTable(table, 0, 23);


                DataSet1.ContainerOccupancyRow row;
                //Parallel.ForEach(table.AsEnumerable(), row =>
                for(int i = 0; i < table.Rows.Count; i++)
                {
                    if (i % 1000 == 0)
                        Console.WriteLine(String.Format("Processed {0} Rows ({1:0}% Complete)",i,(double)i/table.Rows.Count*100.00));

                    row = (DataSet1.ContainerOccupancyRow)table.Rows[i];
                    row.StartDate = row.ActivityStartTimeLocal.StartOfDay();

                    for (int hour = 0; hour <= 23; hour++)
                    {
                        startPeriod = row.StartDate.AddHours(hour);
                        endPeriod = row.StartDate.AddHours(hour + 1);

                        // Record Workpoints & Collaboration Spaces
                        row["WPS" + hour] = row.Workpoints;
                        row["CLS" + hour] = row.Capacity > 0 ? 1 : 0;

                        if (row.IsUtilizedBetween(startPeriod,endPeriod)) 
                        {
                            // UST - Utilisation Start Time
                            if (row.ActivityStartTimeLocal <= startPeriod)
                                utilisationStart = startPeriod;
                            else
                                utilisationStart = row.ActivityStartTimeLocal.NearestMinute();
                            row["UST" + hour] = utilisationStart;

                            // UTI - Utilized
                            row["UTI" + hour] = true;

                            // UET - Utilisation End Time
                            if (row.ActivityEndTimeLocal >= endPeriod)
                                utilisationEnd = endPeriod;
                            else
                                utilisationEnd = row.ActivityEndTimeLocal.NearestMinute();
                            row["UET" + hour] = utilisationEnd;

                            // MIU - Minutes Utilized
                            minutesUtilised = utilisationEnd.Value.MinutesAfter(utilisationStart.Value);
                            row["MIU" + hour] = minutesUtilised;

                            // WPU - Workpoints Utilized
                            // PWU - Peak Workpoints Utilized 
                            // AWU - Average Workpoints Utilized 
                            if (row.Workpoints > 0)
                                row["WPU" + hour] = row["PWU" + hour] = row["AWU" + hour] = row.Workpoints;

                            // CSU - Collaboration Spaces Utilized
                            // PCU - Peak Collaboration Spaces Utilized
                            // ACU - Average Collaboration Spaces Utilized
                            if (row.Capacity > 0)
                                row["CSU" + hour] = row["PCU" + hour] = row["ACU" + hour] = 1;

                            // OCM - Occupant Minutes
                            if (row.OccupancyEndTimeLocal >= endPeriod)
                                occupantMinutes = endPeriod.MinutesAfter(utilisationStart.Value);
                            else
                                occupantMinutes = row.OccupancyEndTimeLocal.NearestMinute().MinutesAfter(utilisationStart.Value);  
                            row["OCM" + hour] = occupantMinutes;


                            //PKO - Peak Occupancy - Populate PersonID This Hour
                            //ATT - Attendance - Populate PersonID This Hour
                            //TODO: Accommodate for Vergesense. 
                            row["PKO" + hour] = row["ATT" + hour] = row.OccupiedByPersonID;
                        }

                    }
                }


                if (table.Rows.Count > 0)
                {
                    // Commit HourlyCacheByContainer
                    var groupedTable = AggregateTable(table, TimeWindow.Hour, Area.Container, HourlyCacheLastUpdatedUTC,AggregationType.Hour);
                    //var groupedTableClone = groupedTable.Copy();
                    //var groupedTable = AggregateTable(table, "ContainerID", "StartDate", "SpaceUtilisationPerHour", 0, 23, HourlyCacheLastUpdatedUTC);
                    CommitCache(HourlyCacheLastUpdatedUTC, groupedTable,TimeWindow.Hour,Area.Container);

                    //GenerateAggregatedLocationCache(Area.Floor, HourlyCacheLastUpdatedUTC, TimeWindow.Hour);
                    // Commit HourlyCacheByFloor
                    //groupedTable = AggregateTable(table, TimeWindow.Hour, Area.Floor, HourlyCacheLastUpdatedUTC, AggregationType.Area);
                    //groupedTable = AggregateTable(table, "FloorID", "StartDate", "SpaceUtilisationByFloorPerHour", 0, 23, HourlyCacheLastUpdatedUTC);
                    //CommitCache(HourlyCacheLastUpdatedUTC, groupedTable, TimeWindow.Hour, Area.Floor);

                    // Commit HourlyCacheByBuilding
                    //groupedTable = AggregateTable(table, TimeWindow.Hour, Area.Building, HourlyCacheLastUpdatedUTC, AggregationType.Area);
                    //groupedTable = AggregateTable(table, "BuildingID", "StartDate", "SpaceUtilisationByBuildingPerHour", 0, 23, HourlyCacheLastUpdatedUTC);
                    //CommitCache(HourlyCacheLastUpdatedUTC, groupedTable, TimeWindow.Hour, Area.Building);

                }

                Console.WriteLine("Process Time: " + stopwatch.Elapsed.ToString());
            }
        }



        private static void GenerateCachesForTimeWindow(TimeWindow window)
        {
            DateTime generateCacheFrom = HourlyCacheLastUpdatedUTC;

            switch (window)
            {
                case TimeWindow.Day:
                case TimeWindow.Weekday:
                    generateCacheFrom = HourlyCacheLastUpdatedUTC.WorldMin();
                    break;
                case TimeWindow.Month:
                    generateCacheFrom = MonthlyCacheLastUpdatedUTC.StartOfMonth();
                    break;
            }


            // Space Cache for Requested Window. 
            if (window == TimeWindow.Hour)
            {
                Console.WriteLine(Environment.NewLine + "GENERATING SPACE CACHE BY HOUR");
                //GenerateHourlyCacheForSpaces();
            }
            else
            {
                Console.WriteLine(Environment.NewLine + "GENERATING SPACE CACHE BY " + window);

                int minValue = GetMinValue(window);
                int maxValue = GetMaxValue(window);

                // Get ContainerOccupancy Data
                StringBuilder sb = new StringBuilder();
                sb.Append(@"SELECT su.ContainerID,StartDate, tz.Name as 'TimeZone',
                DATEFROMPARTS(YEAR(StartDate),1,1) as StartOfYear,
                DATEFROMPARTS(YEAR(StartDate), MONTH(StartDate), 1) as StartofMonth,
                DATEADD(ww, DATEDIFF(ww, 0, StartDate), 0) as StartOfWeek");

                for (int i = 0; i < Metrics.Count; i++)
                {
                    AppendMultiColumnAggregate(sb, Metrics[i], 0, 23);
                }

                sb.Append(" FROM [SpaceUtilisationPerHour] su INNER JOIN [Container] c ON (su.ContainerID = c.ContainerID) INNER JOIN [Building] b ON (c.BuildingID = b.BuildingID) LEFT OUTER JOIN tzdb.Zones tz ON (b.TimeZoneID = tz.ID)  WHERE StartDate >='").Append(generateCacheFrom.ToString("yyyy/MM/dd")).Append("'");

                var table = GetData(sb.ToString());

                AddMetricColumnsToTable(table, minValue, maxValue);

                PivotTable(table, window);

                var tableName = "SpaceUtilisationPer" + window;
                Console.WriteLine("Processing {0}", tableName);

                var aggregatedTable = AggregateTable(table, window, Area.Container, generateCacheFrom, AggregationType.Time);
                CommitCache(generateCacheFrom, aggregatedTable, window, Area.Container);
                //TODO: MAKE SURE WE'RE NOT OVERWRITING PAST DATA
            }

            // Floor Cache for this Window
            Console.WriteLine(String.Format(Environment.NewLine + "**** GENERATING FLOOR CACHE BY {0} ****",window));
            GenerateAreaCacheForWindow(Area.Floor, generateCacheFrom, window);

            // Building Cache for this Window
            Console.WriteLine(String.Format(Environment.NewLine + "**** GENERATING BUILDING CACHE BY {0} ****", window));
            GenerateAreaCacheForWindow(Area.Building, generateCacheFrom, window);
        }

        private static void GenerateAreaCacheForWindow(Area area, DateTime generateCacheFrom, TimeWindow window)
        {
            var tableName = "SpaceUtilisationBy" + area + "Per" + window;

            int minValue = GetMinValue(window);
            int maxValue = GetMaxValue(window);

            DateTime getDataFromStartDate = generateCacheFrom; 
            switch(window)
            {
                case TimeWindow.Hour:
                    getDataFromStartDate = generateCacheFrom.StartOfDay();
                    break;
                case TimeWindow.Day:
                    getDataFromStartDate = generateCacheFrom.StartOfMonth();
                    break;
                case TimeWindow.Weekday:
                    getDataFromStartDate = generateCacheFrom.StartOfWeek();
                    break;
                case TimeWindow.Month:
                    getDataFromStartDate = generateCacheFrom.StartOfYear();
                    break;
            }

            StringBuilder sb = new StringBuilder();

            sb.Append("SELECT ").Append(area).Append("ID,StartDate,");

            var metrics = Metrics.Select(r => r).ToList<Metric>();

            var aggregateFunction = Metric.Function.None;

            bool firstMetric = true;
            for (int i = 0; i < metrics.Count; i++)
            {
                if (area == Area.Container)
                {
                    if (window == TimeWindow.Hour)
                        aggregateFunction = metrics[i].SpaceHourlyFunction;
                    else
                        aggregateFunction = metrics[i].SpaceDailyOrHigherFunction;
                }
                else
                {
                    if (window == TimeWindow.Hour)
                        aggregateFunction = metrics[i].NonSpaceHourlyFunction;
                    else
                        aggregateFunction = metrics[i].NonSpaceDailyOrHigherFunction;
                }

                //if (aggregateFunction != Metric.Function.AvgHourly && aggregateFunction != Metric.Function.PeakHourly)
                //{
                    if (!firstMetric)
                        sb.Append(",").Append(Environment.NewLine);
                    firstMetric = false;

                    for (int v = minValue; v <= maxValue; v++)
                    {
                        if (aggregateFunction == Metric.Function.Or)
                            sb.Append("CAST(MAX(CAST([").Append(metrics[i].Abbreviation).Append(v.ToString("D2")).Append("] as tinyint)) as bit) as ").Append(metrics[i].Abbreviation).Append(v);
                        else if (aggregateFunction == Metric.Function.CountDistinct || aggregateFunction == Metric.Function.AvgHourly || aggregateFunction == Metric.Function.PeakHourly)
                            sb.Append("0 as ").Append(metrics[i].Abbreviation).Append(v);
                        else
                            sb.Append(aggregateFunction).Append("([").Append(metrics[i].Abbreviation).Append(v.ToString("D2")).Append("]) as ").Append(metrics[i].Abbreviation).Append(v);
                        if (v != maxValue)
                            sb.Append(",");
                    }
                //}
            }

            sb.Append(@" FROM SpaceUtilisationPer").Append(window).Append(@" su
                INNER JOIN Container c ON(su.ContainerID = c.ContainerID)
                WHERE StartDate >= '").Append(getDataFromStartDate.ToString("yyyy/MM/dd")).Append(@"'
                GROUP BY ").Append(area).Append("ID, StartDate");

            DataTable table = GetData(sb.ToString());
            table.TableName = tableName;
            table.PrimaryKey = new DataColumn[] { table.Columns[area + "ID"], table.Columns["StartDate"] };

            if (table.Rows.Count == 0)
                return;

            //Get PeakHourly and AverageHourly values
            sb.Clear();
            sb.Append("SELECT su.").Append(area).Append(@"ID,StartDate, tz.Name as 'TimeZone',DATEFROMPARTS(YEAR(StartDate),1,1) as StartOfYear,
                DATEFROMPARTS(YEAR(StartDate), MONTH(StartDate), 1) as StartofMonth,
                DATEADD(ww, DATEDIFF(ww, 0, StartDate), 0) as StartOfWeek,");
            firstMetric = true;
            bool generateTable2 = false;
            string function = String.Empty;
            List<Metric> hourlyMetrics  = new List<Metric>();
            for (int i = 0; i < metrics.Count; i++)
            {
                if (area == Area.Container)
                {
                    if (window == TimeWindow.Hour)
                        aggregateFunction = metrics[i].SpaceHourlyFunction;
                    else
                        aggregateFunction = metrics[i].SpaceDailyOrHigherFunction;
                }
                else
                {
                    if (window == TimeWindow.Hour)
                        aggregateFunction = metrics[i].NonSpaceHourlyFunction;
                    else
                        aggregateFunction = metrics[i].NonSpaceDailyOrHigherFunction;
                }

                if (aggregateFunction == Metric.Function.AvgHourly || aggregateFunction == Metric.Function.PeakHourly)
                {
                    hourlyMetrics.Add(metrics[i]);

                    generateTable2 = true;
                    if (!firstMetric)
                        sb.Append(",").Append(Environment.NewLine);
                    firstMetric = false;

                    switch (aggregateFunction)
                    {
                        case Metric.Function.AvgHourly:
                            function = "AVG";
                            break;
                        case Metric.Function.PeakHourly:
                            function = "MAX";
                            break;
                    }

                    sb.Append("(SELECT ").Append(function).Append("(").Append(metrics[i].Abbreviation).Append(") FROM (VALUES ");
                    for (int j = 0; j <= 23; j++)
                    {
                        sb.Append("(").Append(metrics[i].Abbreviation).Append(j.ToString("D2")).Append(")");
                        if (j != 23)
                            sb.Append(",");
                    }
                    sb.Append(") AS x(").Append(metrics[i].Abbreviation).Append(")) AS ").Append(metrics[i].Abbreviation);
                }
            }



            sb.Append(@" FROM SpaceUtilisationBy").Append(area).Append(@"PerHour su ");
            if (area == Area.Floor)
                sb.Append("INNER JOIN [Floor] f ON (su.FloorID=f.FloorID) INNER JOIN [Building] b ON (f.BuildingID = b.BuildingID) ");
            else if (area == Area.Building)
                sb.Append("INNER JOIN [Building] b ON (su.BuildingID = b.BuildingID) ");
            sb.Append(" LEFT OUTER JOIN tzdb.Zones tz ON (b.TimeZoneID = tz.ID) ");

            sb.Append("WHERE StartDate >= '").Append(getDataFromStartDate.ToString("yyyy/MM/dd")).Append("'");

            DateTime minStartDate = (DateTime)table.Compute("MIN(StartDate)", String.Empty);

            if (generateTable2)
            {
                DataTable table2 = GetData(sb.ToString());
                AddMetricColumnsToTable(table2, minValue, maxValue, hourlyMetrics);
                PivotTable(table2, window,hourlyMetrics);
                
                DataTable aggTable2 = AggregateTable(table2, window, area, generateCacheFrom, AggregationType.Hour,hourlyMetrics);
                aggTable2.TableName = tableName;
                aggTable2.PrimaryKey = new DataColumn[] { aggTable2.Columns[area + "ID"], aggTable2.Columns["StartDate"] };
                table.Merge(aggTable2);
            }


            int totalTimeWindows = 0;
            switch (window)
            {
                case TimeWindow.Hour:
                case TimeWindow.Day:
                case TimeWindow.Weekday:
                    totalTimeWindows = Convert.ToInt32(CurrentTimeUTC.Date.Subtract(generateCacheFrom.Date).TotalDays);
                    break;
                case TimeWindow.Month:
                    totalTimeWindows = Convert.ToInt32(CurrentTimeUTC.Date.Subtract(generateCacheFrom.Date).TotalDays / 28);
                    break;
            }

            DataTable attendanceTable;
            for (int timeWindow = 0; timeWindow <= totalTimeWindows; timeWindow++)
            {
                attendanceTable = null;
                switch (window)
                {
                    case TimeWindow.Hour:
                    case TimeWindow.Day:
                    case TimeWindow.Weekday:
                        attendanceTable = GetAttendanceData(generateCacheFrom.AddDays(timeWindow), generateCacheFrom.AddDays(timeWindow).AddHours(36), window, area);
                        break;
                    case TimeWindow.Month:
                        attendanceTable = GetAttendanceData(generateCacheFrom.AddMonths(timeWindow), generateCacheFrom.AddMonths(timeWindow + 1).AddHours(36), window, area);
                        break;
                }
                if (attendanceTable != null)
                {
                    table.Merge(attendanceTable);
                }
            }

            for (int i = 0; i < table.Rows.Count; i++)
                if ((DateTime)table.Rows[i]["StartDate"] < minStartDate)
                    table.Rows[i].Delete();
            table.AcceptChanges();


            CommitCache(generateCacheFrom, table, window,area);

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
            PivotTable(table, aggregation, Metrics);
        }



        private static void PivotTable(DataTable table, TimeWindow aggregation, List<Metric> metrics)
        {
            foreach (DataRow row in table.Rows)
            {
                foreach (Metric metric in metrics)
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

        private static string GetTableName(Area area, TimeWindow window)
        {
            var byString = area == Area.Container ? String.Empty : "By" + area.ToString();
            var tableName = String.Format("SpaceUtilisation{0}Per{1}", byString, window.ToString());
            return tableName;
        }

        private static string GetGroupBy(TimeWindow window)
        {
            var groupBy = "";
            switch (window)
            {
                case TimeWindow.Hour:
                    groupBy = "StartDate";
                    break;
                case TimeWindow.Day:
                    groupBy = "StartOfMonth";
                    break;
                case TimeWindow.Weekday:
                    groupBy = "StartOfWeek";
                    break;
                case TimeWindow.Month:
                    groupBy = "StartOfYear";
                    break;
            }
            return groupBy;
        }



        private static DataTable AggregateTable(DataTable sourceTable, TimeWindow window, Area area,  DateTime generateCacheFrom, AggregationType aggregationType)
        {
            return AggregateTable(sourceTable, window, area, generateCacheFrom, aggregationType,Metrics);
        }

        private static DataTable AggregateTable(DataTable sourceTable, TimeWindow window, Area area, DateTime generateCacheFrom, AggregationType aggregationType, List<Metric> metrics)
        {
            if (sourceTable.Rows.Count == 0)
                return null;

            var groupByLocationID = area + "ID";
            var minValue = GetMinValue(window);
            var maxValue = GetMaxValue(window);
            var groupByDate = GetGroupBy(window);
            var newTableName = GetTableName(area, window);

            DataTable newTable = new DataTable();
            newTable.Columns.Add(groupByLocationID, typeof(int));
            newTable.Columns.Add("StartDate", typeof(DateTime));
            newTable.Columns.Add("MinCachedDateLocal", typeof(DateTime));

            AddMetricColumnsToTable(newTable, minValue, maxValue, metrics);
            Metric.Function function = Metric.Function.None;

            var groupedTable = sourceTable.AsEnumerable()
                            .GroupBy(r => new { LocationID = r.Field<int>(groupByLocationID), StartDate = r.Field<DateTime>(groupByDate), Timezone = r.Field<string>("Timezone") })
                            .Select(g =>
                            {
                                var newRow = newTable.NewRow();

                                newRow[groupByLocationID] = g.Key.LocationID;
                                newRow["StartDate"] = g.Key.StartDate;
                                newRow["MinCachedDateLocal"] = generateCacheFrom.ValueInTimezone(g.Key.Timezone);// Instant.FromDateTimeUtc(DateTime.SpecifyKind(generateCacheFrom, DateTimeKind.Utc)).InZone(timeZone).ToDateTimeUnspecified(); ;

                                for (int timeWindow = minValue; timeWindow <= maxValue; timeWindow++)
                                {
                                    foreach (Metric metric in metrics)
                                    {
                                        if (area == Area.Container)
                                        {
                                            if (window == TimeWindow.Hour)
                                                function = metric.SpaceHourlyFunction;
                                            else
                                                function = metric.SpaceDailyOrHigherFunction;
                                        }
                                        else
                                        {
                                            if (window == TimeWindow.Hour)
                                                function = metric.NonSpaceHourlyFunction;
                                            else
                                                function = metric.NonSpaceDailyOrHigherFunction;
                                        }
                                        switch (function)
                                        {
                                            case Metric.Function.Or:
                                                newRow[metric.Abbreviation + timeWindow] = g.Where(r => r.Field<bool>(metric.Abbreviation + timeWindow) == true).Count() > 0;
                                                break;
                                            case Metric.Function.Sum:
                                                newRow[metric.Abbreviation + timeWindow] = g.Sum(r => r.Field<int>(metric.Abbreviation + timeWindow));
                                                break;
                                            case Metric.Function.PeakHourly:
                                            case Metric.Function.AvgHourly:
                                            case Metric.Function.Max:
                                                if (metric.AllowNulls && metric.DataType == typeof(DateTime))
                                                {
                                                    DateTime? value = g.Max(r => r.Field<DateTime?>(metric.Abbreviation + timeWindow));
                                                    if (value != null)
                                                        newRow[metric.Abbreviation + timeWindow] = value;
                                                }
                                                else
                                                    newRow[metric.Abbreviation + timeWindow] = g.Max(r => r.Field<int>(metric.Abbreviation + timeWindow));
                                                break;
                                            case Metric.Function.Min:
                                                if (metric.AllowNulls && metric.DataType == typeof(DateTime))
                                                {
                                                    DateTime? value = g.Min(r => r.Field<DateTime?>(metric.Abbreviation + timeWindow));
                                                    if (value != null)
                                                        newRow[metric.Abbreviation + timeWindow] = value;
                                                }
                                                else
                                                    newRow[metric.Abbreviation + timeWindow] = g.Min(r => r.Field<int>(metric.Abbreviation + timeWindow));
                                                break;
                                            case Metric.Function.CountDistinct:
                                                newRow[metric.Abbreviation + timeWindow] = g.Where(r => r.Field<int>(metric.Abbreviation + timeWindow) != 0).Select(r => r.Field<int>(metric.Abbreviation + timeWindow)).Distinct().Count();
                                                break;

                                        }
                                    }

                                }

                                return newRow;

                            })
                            .CopyToDataTable();

            groupedTable.TableName = newTableName;
            groupedTable.PrimaryKey = new DataColumn[] { groupedTable.Columns[groupByLocationID], groupedTable.Columns["StartDate"] };

            return groupedTable;

        }

        private static void PivotValue(DataRow row, Metric metric, TimeWindow aggregation)
        {
            DateTime startDate = (DateTime)row["StartDate"];
            if (metric.NonSpaceDailyOrHigherFunction == Metric.Function.CountDistinct || metric.NonSpaceDailyOrHigherFunction == Metric.Function.None)
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


            if (metric.SpaceDailyOrHigherFunction == Metric.Function.Or)
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
            else if (metric.SpaceDailyOrHigherFunction != Metric.Function.CountDistinct && metric.SpaceDailyOrHigherFunction != Metric.Function.None)
            {
                sb.Append(",(SELECT ").Append(metric.SpaceDailyOrHigherFunction.ToString()).Append("(").Append(metric.Abbreviation).Append(") FROM (VALUES ");
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

        private static DataTable GetAttendanceData(DateTime generateCacheFrom, DateTime generateCacheTo, TimeWindow window, Area area)
        {
           
            int minValue = GetMinValue(window);
            int maxValue = GetMaxValue(window);
            var locationID = String.Format("{0}ID", area);
            var byString = area == Area.Container ? String.Empty : "By" + area.ToString();
            var tableName = String.Format("SpaceUtilisation{0}Per{1}", byString, window.ToString());


            // Get ContainerOccupancy Data To Process
            DateTime processDate = generateCacheFrom;
            Console.WriteLine("Retrieving Attendance Data for {0}", generateCacheFrom);
            var table = GetContainerOccupancyData(processDate, generateCacheTo);

            AddLocalTimezonesAndHelperDates(table);
            var attMetric = new Metric("Attendance", "ATT", typeof(int), Metric.SqlType.Smallint, false, Metric.Function.CountDistinct);
            AddMetricColumns(table, attMetric, minValue, maxValue);
            table.Columns.Add("StartOfMonth", typeof(DateTime));
            table.Columns.Add("StartOfWeek", typeof(DateTime));
            table.Columns.Add("StartOfYear", typeof(DateTime));
            table.Columns.Add("StartOfDay", typeof(DateTime));

            DataSet1.ContainerOccupancyRow row;
            DateTime startPeriod = DateTime.Now;
            DateTime endPeriod = DateTime.Now;

            for (int i = 0; i < table.Rows.Count; i++)
            {
                if (i % 1000 == 0)
                    Console.WriteLine("Processed " + i + " rows");

                row = (DataSet1.ContainerOccupancyRow)table.Rows[i];
                switch(window)
                {
                    case TimeWindow.Hour:
                        row.StartDate = new DateTime(row.ActivityStartTimeLocal.Year, row.ActivityStartTimeLocal.Month, row.ActivityStartTimeLocal.Day);
                        row["StartOfDay"] = row.StartDate.StartOfDay();
                        break;
                    case TimeWindow.Day:
                        row.StartDate = new DateTime(row.ActivityStartTimeLocal.Year, row.ActivityStartTimeLocal.Month, 1);
                        row["StartOfMonth"] = row.StartDate.StartOfMonth();
                        break;
                    case TimeWindow.Weekday:
                        row.StartDate = new DateTime(row.ActivityStartTimeLocal.Year, row.ActivityStartTimeLocal.Month, row.ActivityStartTimeLocal.Day - Convert.ToInt32(row.ActivityStartTimeLocal.DayOfWeek));
                        row["StartOfWeek"] = row.StartDate.StartOfWeek();
                        break;
                    case TimeWindow.Month:
                        row.StartDate = new DateTime(row.ActivityStartTimeLocal.Year, 1, 1);
                        row["StartOfYear"] = row.StartDate.StartOfYear();
                        break;
                }

                for (int v = minValue; v <= maxValue; v++)
                {

                    
                    switch(window)
                    {
                        case TimeWindow.Hour:
                            startPeriod = row.StartDate.AddHours(v);
                            endPeriod = row.StartDate.AddHours(v + 1);
                            break;
                        case TimeWindow.Day:
                        case TimeWindow.Weekday:
                            startPeriod = row.StartDate.AddDays(v);
                            endPeriod = row.StartDate.AddDays(v + 1);
                            break;
                        case TimeWindow.Month:
                            startPeriod = row.StartDate.AddMonths(v);
                            endPeriod = row.StartDate.AddMonths(v + 1);
                            break;
                    }

                    if (row.ActivityEndTimeLocal > startPeriod && row.ActivityStartTimeLocal <= endPeriod) // isUtilized
                        row["ATT" + v] = row.OccupiedByPersonID;
                }
            }

            var groupedTable = AggregateTable(table, window, area, generateCacheFrom, AggregationType.Area, new List<Metric> { attMetric });
            //var groupedTable = AggregateTable(table, locationID, "StartDate", tableName, minValue, maxValue, processDate, new List<Metric> { attMetric });


            return groupedTable;

        }

        private static void CommitCache(DateTime generateCacheFrom, DataTable table, TimeWindow window, Area area)
        {
            var minValue = GetMinValue(window);
            var maxValue = GetMaxValue(window);

            string locationID = area + "ID";

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

            if (table.Columns.Contains("MinCachedDateLocal"))
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

        private static DataSet1.ContainerOccupancyDataTable GetContainerOccupancyData(DateTime fromDateUTC, DateTime toDateUTC)
        {
            Console.WriteLine("Retrieving ContainerOccupancyData from {0} to {1} UTC", fromDateUTC, toDateUTC);
            var table = new DataSet1.ContainerOccupancyDataTable();

            using (SqlConnection sqlConn = new SqlConnection(ConnectionString))
            {
                //TODO: TIMEZONES - Should all be stored in UTC in the ContainerOccupancyTable. 
                //NOTE: We remove any utilization blips less IGNORE_UTILIZATION_BLIP_LESS_THAN_MINS here... no point retrieving them from the database if they're not needed.   
                string sqlQuery = String.Format(@"DECLARE @fromDateUTC datetime = '{0:yyyy/MM/dd HH:mm}'
                    DECLARE @toDateUTC datetime = '{1:yyyy/MM/dd HH:mm}'
                    DECLARE @MinUtilisationMins as INT = {2}

                     
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
		                    (ISNULL(LogoutTimeUTC,GETUTCDATE()) >= @fromDateUTC) -- Only select rows that were logged out since RegenerateFromLogoutTime
		                    AND LoginTimeUTC <= @toDateUTC
		                    AND DATEDIFF(mi,LoginTimeUTC,ISNULL(LogoutTimeUTC,GETUTCDATE())) > @MinUtilisationMins -- Remove rows below Utilization Threshold
                    ) as a ", fromDateUTC,toDateUTC, IGNORE_UTILIZATION_BLIP_LESS_THAN_MINS);

                using (SqlCommand cmd = new SqlCommand(sqlQuery, sqlConn))
                {
                    cmd.CommandTimeout = 300;
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
