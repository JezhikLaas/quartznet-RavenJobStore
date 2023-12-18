using Raven.Client.Documents.Session;

namespace Domla.Quartz.Raven.Strategies;

internal interface IBlockRepository
{
    Task BlockJobAsync(IAsyncDocumentSession session, string jobId, CancellationToken token);
    
    Task ReleaseJobAsync(IAsyncDocumentSession session, string jobId, CancellationToken token);
    
    Task<bool> IsJobBlockedAsync(IAsyncDocumentSession session, string jobId, CancellationToken token);
    
    Task<IReadOnlyList<string>> GetBlockedJobsAsync(IAsyncDocumentSession session, CancellationToken token);
    
    Task ReleaseAllJobsAsync(IAsyncDocumentSession session, CancellationToken token);
}