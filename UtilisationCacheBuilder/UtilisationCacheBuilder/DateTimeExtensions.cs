using NodaTime;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace UtilisationCacheBuilder
{
    public static class DateTimeExtensions
    {
        public static DateTime StartOfHour(this DateTime dt)
        {
            return new DateTime(dt.Year, dt.Month, dt.Day, dt.Hour, 0, 0);
        }

        public static DateTime StartOfYear(this DateTime dt)
        {
            return new DateTime(dt.Year, 1, 1, 0, 0, 0);
        }

        public static DateTime StartOfDay(this DateTime dt)
        {
            return new DateTime(dt.Year, dt.Month, dt.Day);
        }

        // Serraview considers Monday the Start of the Week, with Monday=1 and Sunday=7  
        public static DateTime StartOfWeek(this DateTime dt)
        {
            var dayNo = dt.DayOfWeek == DayOfWeek.Sunday ? 7 : Convert.ToInt32(dt.DayOfWeek);
            var newDate = dt.AddDays( dayNo * -1 );
            newDate = new DateTime(newDate.Year, newDate.Month, newDate.Day);
            return newDate;
        }

        public static DateTime StartOfMonth(this DateTime dt)
        {
            return new DateTime(dt.Year, dt.Month, 1);
        }

        public static DateTime WorldMax(this DateTime dt)
        {
            return dt.AddHours(14);
        }

        public static DateTime WorldMin(this DateTime dt)
        {
            return dt.AddHours(-12);
        }

        public static int MinutesAfter(this DateTime dt, DateTime startDate)
        {
            return Convert.ToInt32(Math.Ceiling(dt.Subtract(startDate).TotalMinutes));
        }

        public static DateTime ValueInTimezone(this DateTime dt, string timezone)
        {
            var timeZone = DateTimeZoneProviders.Tzdb[timezone];
            return Instant.FromDateTimeUtc(DateTime.SpecifyKind(dt, DateTimeKind.Utc)).InZone(timeZone).ToDateTimeUnspecified();
        }

        public static DateTime NearestMinute(this DateTime dt)
        {
            var newDT = new DateTime(dt.Year, dt.Month, dt.Day, dt.Hour, dt.Minute, 0);
            if (dt.Second >= 30)
                newDT = newDT.AddMinutes(1);
            return newDT;
        }






    }
}
