using FluentAssertions;
using Newtonsoft.Json;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;

namespace Quartz.Impl.UnitTests;

public class RavenTests : TestBase
{
    [Fact(DisplayName = "If a document is modified Then streaming it sees the changes")]
    public async Task If_a_document_is_modified_Then_streaming_it_sees_the_changes()
    {
        using var store = CreateStore();
        
        // Create the document
        using (var session = store.OpenAsyncSession())
        {
            var test = new Test { Value = "one" };
            await session.StoreAsync(test);

            await session.SaveChangesAsync();
        }

        // Modify the document
        using (var session = store.OpenAsyncSession())
        {
            var test = await session.Query<Test>().FirstAsync();
            test.Value = "two";

            await session.SaveChangesAsync();
        }
        
        // Read via streaming API
        using (var session = store.OpenAsyncSession())
        {
            var query = session.Query<Test>();

            await using var stream = await session
                .Advanced
                .StreamAsync(query);

            var found = await stream.MoveNextAsync();
            found.Should().BeTrue();

            stream.Current.Document.Value.Should().Be("two");
        }
    }

    [Fact(DisplayName = "If a document is modified Then streaming it by index sees the changes")]
    public async Task If_a_document_is_modified_Then_streaming_it_by_index_sees_the_changes()
    {
        using var store = CreateStore();
        await store.ExecuteIndexAsync(new TestIndex());
        
        // Create the document
        using (var session = store.OpenAsyncSession())
        {
            var test = new Test { Value = "one", Category = "XXX" };
            await session.StoreAsync(test);

            await session.SaveChangesAsync();
        }

        // Modify the document
        using (var session = store.OpenAsyncSession())
        {
            var test = await session.Query<Test>().FirstAsync(x => x.Category == "XXX");
            test.Value = "two";

            await session.SaveChangesAsync();
        }
        
        // Read via streaming API
        using (var session = store.OpenAsyncSession())
        {
            var query = session
                .Query<Test>(nameof(TestIndex))
                .Where(x => x.Category == "XXX");

            await using var stream = await session
                .Advanced
                .StreamAsync(query);

            var found = await stream.MoveNextAsync();
            found.Should().BeTrue();

            stream.Current.Document.Value.Should().Be("two");
        }
    }
}

public class Test
{
    [JsonProperty]
    public string? Value { get; set; }

    [JsonProperty]
    public string? Category { get; init; }
}

public class TestIndex : AbstractIndexCreationTask<Test>
{
    public TestIndex()
    {
        Map = tests => from test in tests
            select new
            {
                test.Category
            };
    }
}