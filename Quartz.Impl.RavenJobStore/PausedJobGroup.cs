namespace Quartz.Impl.RavenJobStore;

internal class PausedJobGroup
{
    internal PausedJobGroup(string scheduler, string group)
    {
        Scheduler = scheduler;
        GroupName = group;
        Id = GetId(scheduler, group);
    }
    
    public static string GetId(string scheduler, string group) =>
        $"J{scheduler}#{group}";
    
    // ReSharper disable once AutoPropertyCanBeMadeGetOnly.Global
    public string Id { get; set; }
    
    public string Scheduler { get; set; }
    
    public string GroupName { get; set; }
}