using Quartz.Impl.RavenJobStore.Entities;
using Raven.Client.Documents.Indexes;

namespace Quartz.Impl.RavenJobStore.Indexes;

internal class JobGroupsIndex : AbstractIndexCreationTask<Job> 
{
    internal class Result
    {
        public string Scheduler { get; init; } = null!;
        
        public string Group { get; init; } = null!;
    }

    internal JobGroupsIndex()
    {
        Map = jobs => from job in jobs
            select new Result
            {
                Scheduler = job.Scheduler,
                Group = job.Group
            };

        Reduce = results => from result in results
            group result by new { result.Scheduler, result.Group }
            into resultGroup
            select new Result
            {
                Scheduler = resultGroup.Key.Scheduler,
                Group = resultGroup.Key.Group
            };
    }
}