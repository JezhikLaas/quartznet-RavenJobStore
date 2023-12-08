using Quartz.Impl.RavenJobStore.Entities;
using Raven.Client.Documents.Indexes;

namespace Quartz.Impl.RavenJobStore.Indexes;

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