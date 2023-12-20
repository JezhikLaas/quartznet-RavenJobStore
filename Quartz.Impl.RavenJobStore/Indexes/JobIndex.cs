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
                job.RequestsRecovery,
                job.Group
            };
    }
}