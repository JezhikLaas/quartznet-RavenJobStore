using System.Collections.Concurrent;
using Domla.Quartz.Raven.Strategies;
using Raven.Client.Documents.Session;

namespace Domla.Quartz.Raven.ConcreteStrategies;

internal class MemoryBlockRepository : IBlockRepository
{
    private ConcurrentDictionary<string, byte> BlockedJobs { get; } = new(); 
    
    public Task BlockJobAsync(IAsyncDocumentSession session, string jobId, CancellationToken token)
    {
        BlockedJobs.TryAdd(jobId, 0);
        return Task.CompletedTask;
    }

    public Task ReleaseJobAsync(IAsyncDocumentSession session, string jobId, CancellationToken token)
    {
        BlockedJobs.TryRemove(jobId, out _);
        return Task.CompletedTask;
    }

    public Task<bool> IsJobBlockedAsync(IAsyncDocumentSession session, string jobId, CancellationToken token) => 
        Task.FromResult(BlockedJobs.ContainsKey(jobId));

    public Task<IReadOnlyList<string>> GetBlockedJobsAsync(IAsyncDocumentSession session, CancellationToken token) =>
        Task.FromResult<IReadOnlyList<string>>(BlockedJobs.Keys.ToList());

    public Task ReleaseAllJobsAsync(IAsyncDocumentSession session, CancellationToken token)
    {
        BlockedJobs.Clear();
        return Task.CompletedTask;
    }
}