namespace Domla.Quartz.Raven;

public static class Guards
{
    public static int MustBeGreaterThan(this int value, int limit)
    {
        if (value <= limit) throw new ArgumentException($"must be greater than {limit}", nameof(value));
        return value;
    }
    
    public static int MustBeGreaterThanOrEqualTo(this int value, int limit)
    {
        if (value < limit)
        {
            throw new ArgumentException($"must be greater than or equal to {limit}", nameof(value));
        }
        
        return value;
    }
}