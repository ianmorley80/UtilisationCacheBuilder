namespace UtilisationCacheBuilder
{


    partial class DataSet1
    {
        partial class MetricDataDataTable
        {
        }

        partial class ContainerOccupancyDataTable
        {
        }

        partial class ContainerOccupancyRow
        {
            public bool ActivityStartsPreviousDay
            {
                get
                {
                    return ActivityStartTimeLocal.Date < StartDate.Date;
                }

            }

            public bool UtilisationEndsToday
            {
                get
                {
                    return UtilisationEndTimeLocal.Date == StartDate.Date;
                }
            }

            public bool ActivityStartedToday
            {
                get
                {
                    return ActivityStartTimeLocal.Date == StartDate.Date;
                }
            }

            public bool OccupancyEndsToday
            {
                get
                {
                    return OccupancyEndTimeLocal.Date == StartDate.Date;
                }
            }

            public bool OccupancyEndsFutureDay
            {
                get
                {
                    return OccupancyEndTimeLocal.Date > StartDate.Date;
                }
            }

            public bool OccupancyStartsPreviousDay
            {
                get
                {
                    return ActivityStartTimeLocal.Date < StartDate.Date;
                }
            }

            public bool UtilisationEndsFutureDay
            {
                get
                {
                    return UtilisationEndTimeLocal.Date > StartDate.Date;
                }
            }

        }
    }
}
