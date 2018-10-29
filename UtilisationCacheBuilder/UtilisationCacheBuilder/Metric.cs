using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace UtilisationCacheBuilder
{
    public class Metric
    {
        public Metric(string name, string abbreviation, Type datatype, SqlType sqlType, bool allowNulls, Function function)
        {
            Name = name;
            Abbreviation = abbreviation;
            DataType = datatype;
            SqlDataType = sqlType;
            AllowNulls = allowNulls;
            AggregateFunction = function;
        }

        public enum Function { Sum, Avg, Max, Min, Or, CountDistinct }
        public enum SqlType { Smalldatetime, Bit, Int, Smallint }

        public string Name { get; set; }
        public string Abbreviation { get; set; }
        public Function AggregateFunction { get; set; }
        public Type DataType { get; set; }
        public SqlType SqlDataType { get; set; }
        public bool AllowNulls { get; set; }
    }
}
