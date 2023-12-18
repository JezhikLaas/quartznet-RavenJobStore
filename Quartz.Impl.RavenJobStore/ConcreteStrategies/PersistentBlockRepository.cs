using Domla.Quartz.Raven.Entities;
using Domla.Quartz.Raven.Indexes;
using Domla.Quartz.Raven.Strategies;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;

namespace Domla.Quartz.Raven.ConcreteStrategies;

internal class PersistentBlockRepository : IBlockRepository
{
    private string InstanceName { get; }

    public PersistentBlockRepository(string instanceName)
    {
        InstanceName = instanceName;
    }
    
    public Task BlockJobAsync(IAsyncDocumentSession session, string jobId, CancellationToken token) =>
        session.StoreAsync(new BlockedJob(InstanceName, jobId), token);

    public Task ReleaseJobAsync(IAsyncDocumentSession session, string jobId, CancellationToken token)
    {
        session.Delete(BlockedJob.GetId(InstanceName, jobId));
        return Task.CompletedTask;
    }

    public Task<bool> IsJobBlockedAsync(IAsyncDocumentSession session, string jobId, CancellationToken token) => 
        session.Advanced.ExistsAsync(BlockedJob.GetId(InstanceName, jobId), token);

    public async Task<IReadOnlyList<string>> GetBlockedJobsAsync(
        IAsyncDocumentSession session,
        CancellationToken token) =>
        await (
            from blocked in session.Query<BlockedJob>(nameof(BlockedJobIndex))
            where blocked.Scheduler == InstanceName
            select blocked.JobId
        ).ToListAsync(token).ConfigureAwait(false);

    public async Task ReleaseAllJobsAsync(IAsyncDocumentSession session, CancellationToken token)
    {
        var ids = await (
            from blocked in session.Query<BlockedJob>(nameof(BlockedJobIndex))
            where blocked.Scheduler == InstanceName
            select blocked.Id
        ).ToListAsync(token).ConfigureAwait(false);
        
        ids.ForEach(session.Delete);
    }
}