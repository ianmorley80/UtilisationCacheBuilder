using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace UtilisationCacheBuilder
{
    public class Metric
    {
        public Metric(string name, string abbreviation, Type datatype, SqlType sqlType, bool allowNulls, Function timeFunction, Function locationFunction)
        {
            Name = name;
            Abbreviation = abbreviation;
            DataType = datatype;
            SqlDataType = sqlType;
            AllowNulls = allowNulls;
            TimeAggregateFunction = timeFunction;
            LocationAggregateFunction = locationFunction;
        }

        public enum Function { Sum, Avg, Max, Min, Or, CountDistinct, None }
        public enum SqlType { Smalldatetime, Bit, Int, Smallint }

        public string Name { get; set; }
        public string Abbreviation { get; set; }
        public Function TimeAggregateFunction { get; set; }
        public Function LocationAggregateFunction { get; set; }
        public Type DataType { get; set; }
        public SqlType SqlDataType { get; set; }
        public bool AllowNulls { get; set; }
    }
}
