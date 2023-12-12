using Domla.Quartz.Raven.Entities;
using Raven.Client.Documents.Indexes;

namespace Domla.Quartz.Raven.Indexes;

internal class TriggerIndex : AbstractIndexCreationTask<Trigger>
{
    internal TriggerIndex()
    {
        Map = triggers => from trigger in triggers
            select new
            {
                trigger.Id,
                trigger.Scheduler,
                trigger.CalendarId,
                trigger.JobId,
                trigger.State,
                trigger.NextFireTimeUtc,
                trigger.Priority
            };
    }
}