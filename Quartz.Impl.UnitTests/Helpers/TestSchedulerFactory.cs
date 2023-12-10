using System.Collections.Specialized;
using System.Reflection;

namespace Quartz.Impl.UnitTests.Helpers;

/// <summary>
/// The StdSchedulerFactory of Quartz does not allow for multiple schedulers
/// with the same name in the same process. We wan to unit test clustered
/// schedulers so we have to use our own factory.
/// </summary>
public class TestSchedulerFactory : StdSchedulerFactory
{
    public TestSchedulerFactory(NameValueCollection properties)
        : base(properties)
    { }
    
    public override async Task<IScheduler> GetScheduler(CancellationToken cancellationToken = new())
    {
        var instantiateMethod = typeof(StdSchedulerFactory).GetMethod
        (
            "Instantiate",
            BindingFlags.Instance | BindingFlags.NonPublic,
            Type.EmptyTypes
        );

        var task = (Task<IScheduler>)instantiateMethod!.Invoke(this, null)!;
        await task.ConfigureAwait(false);
        
        return task.Result;
    }
}