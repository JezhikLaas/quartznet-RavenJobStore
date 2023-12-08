using Quartz.Impl.RavenJobStore.Entities;
using Raven.Client.Documents.Indexes;

namespace Quartz.Impl.RavenJobStore.Indexes;

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