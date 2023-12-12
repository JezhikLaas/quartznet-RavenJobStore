using Domla.Quartz.Raven.Entities;
using Raven.Client.Documents.Indexes;

namespace Domla.Quartz.Raven.Indexes;

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