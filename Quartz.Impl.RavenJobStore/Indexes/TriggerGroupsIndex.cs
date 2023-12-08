using Quartz.Impl.RavenJobStore.Entities;
using Raven.Client.Documents.Indexes;

namespace Quartz.Impl.RavenJobStore.Indexes;

internal class TriggerGroupsIndex : AbstractIndexCreationTask<Trigger> 
{
    internal class Result
    {
        public string Scheduler { get; init; } = null!;
        
        public string Group { get; init; } = null!;
    }

    internal TriggerGroupsIndex()
    {
        Map = triggers => from trigger in triggers
            select new Result
            {
                Scheduler = trigger.Scheduler,
                Group = trigger.Group
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