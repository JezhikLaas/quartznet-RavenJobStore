using Domla.Quartz.Raven.Entities;
using Raven.Client.Documents.Indexes;

namespace Domla.Quartz.Raven.Indexes;

internal class JobIndex : AbstractIndexCreationTask<Job>
{
    internal JobIndex()
    {
        Map = jobs => from job in jobs
            select new
            {
                job.Scheduler,
                job.RequestsRecovery
            };
    }
}
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