using Domla.Quartz.Raven.Entities;
using Raven.Client.Documents.Indexes;

namespace Domla.Quartz.Raven.Indexes;

internal class BlockedJobIndex : AbstractIndexCreationTask<BlockedJob>
{
    internal BlockedJobIndex()
    {
        Map = blockedJobs => from blockedJob in blockedJobs
            select new
            {
                blockedJob.Scheduler
            };
    }
}