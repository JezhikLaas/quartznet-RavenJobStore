using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Quartz.Impl.RavenJobStore;

#nullable enable

public static class NullCheck
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void ThrowIfNull(
        [NotNull]this object? instance,
        [CallerArgumentExpression("instance")] string? name = null,
        [CallerFilePath] string? filename = null,
        [CallerLineNumber] int line = 0)
    {
        if (instance == null) throw new ArgumentNullException(name, $"Must not be null at {line} in {filename}");
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [return: NotNullIfNotNull("instance")]
    public static T ThrowIfNull<T>(
        [NotNull]this T? instance,
        [CallerArgumentExpression("instance")] string? name = null,
        [CallerFilePath] string? filename = null,
        [CallerLineNumber] int line = 0)
    {
        if (instance == null) throw new ArgumentNullException(name, $"Must not be null at {line} in {filename}");
        return instance;
    }
}

#nullable restore