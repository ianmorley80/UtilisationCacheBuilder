using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace UtilisationCacheBuilder
{
    public class Metric
    {
        public Metric(string name,string abbreviation, Type datatype, SqlType sqlType, bool allowNulls, Function function)
        {
            Init(name, abbreviation, datatype, sqlType, allowNulls, function, function, function, function);
        }

        public Metric(string name, string abbreviation, Type datatype, SqlType sqlType, bool allowNulls, Function spaceHourlyFunction, Function spaceDailyOrHigherFunction, Function nonSpaceHourlyFunction, Function nonSpaceDailyOrHigherFunction)
        {
            Init(name, abbreviation, datatype, sqlType, allowNulls, spaceHourlyFunction, spaceDailyOrHigherFunction, nonSpaceHourlyFunction, nonSpaceDailyOrHigherFunction);
        }

        private void Init(string name, string abbreviation, Type datatype, SqlType sqlType, bool allowNulls, Function spaceHourlyFunction, Function spaceDailyOrHigherFunction, Function nonSpaceHourlyFunction, Function nonSpaceDailyOrHigherFunction)
        {
            Name = name;
            Abbreviation = abbreviation;
            DataType = datatype;
            SqlDataType = sqlType;
            AllowNulls = allowNulls;
            SpaceHourlyFunction = spaceHourlyFunction;
            SpaceDailyOrHigherFunction = spaceDailyOrHigherFunction;
            NonSpaceHourlyFunction = nonSpaceHourlyFunction;
            NonSpaceDailyOrHigherFunction = nonSpaceDailyOrHigherFunction;

        }

        public enum Function { Sum, Avg, Max, Min, Or, CountDistinct, PeakHourly, AvgHourly, None }
        public enum SqlType { Smalldatetime, Bit, Int, Smallint }

        public string Name { get; set; }
        public string Abbreviation { get; set; }
        public Function SpaceHourlyFunction { get; set; }
        public Function SpaceDailyOrHigherFunction { get; set; }
        public Function NonSpaceHourlyFunction { get; set; }
        public Function NonSpaceDailyOrHigherFunction { get; set; }

        public Type DataType { get; set; }
        public SqlType SqlDataType { get; set; }
        public bool AllowNulls { get; set; }
    }
}
