using Raven.Client.Documents.Indexes;

namespace Domla.Quartz.Raven.Indexes;

internal class CalendarIndex : AbstractIndexCreationTask<Entities.Calendar>
{
    internal CalendarIndex()
    {
        Map = calendars => from calendar in calendars
            select new
            {
                calendar.Scheduler
            };
    }
}