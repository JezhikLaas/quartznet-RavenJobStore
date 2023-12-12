using Domla.Quartz.Raven.Entities;
using Raven.Client.Documents.Indexes;

namespace Domla.Quartz.Raven.Indexes;

internal class PausedTriggerGroupIndex : AbstractIndexCreationTask<PausedTriggerGroup>
{
    internal PausedTriggerGroupIndex()
    {
        Map = pausedTriggerGroups => from triggerGroup in pausedTriggerGroups
            select new
            {
                triggerGroup.Scheduler
            };
    }
}