using Quartz.Impl.RavenJobStore.Entities;
using Raven.Client.Documents.Indexes;

namespace Quartz.Impl.RavenJobStore.Indexes;

internal class PausedJobGroupIndex : AbstractIndexCreationTask<PausedJobGroup>
{
    internal PausedJobGroupIndex()
    {
        Map = pausedJobGroups => from jobGroup in pausedJobGroups
            select new
            {
                jobGroup.Scheduler
            };
    }
}