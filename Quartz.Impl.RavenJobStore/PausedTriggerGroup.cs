namespace Quartz.Impl.RavenJobStore;

internal class PausedTriggerGroup
{
    internal PausedTriggerGroup(string scheduler, string group)
    {
        Scheduler = scheduler;
        GroupName = group;
        Id = GetId(scheduler, group);
    }

    public static string GetId(string scheduler, string group) =>
        $"T{scheduler}#{group}";
    
    // ReSharper disable once AutoPropertyCanBeMadeGetOnly.Global
    public string Id { get; set; }
    
    // ReSharper disable once AutoPropertyCanBeMadeGetOnly.Global
    public string Scheduler { get; set; }
    
    // ReSharper disable once AutoPropertyCanBeMadeGetOnly.Global
    public string GroupName { get; set; }
}