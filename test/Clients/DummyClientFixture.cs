using PipServices3.Commons.Data;
using System;
using System.Threading.Tasks;
using Xunit;

namespace PipServices3.Azure.Clients
{
    [Collection("Sequential")]
    public class DummyClientFixture
    {
        protected IDummyClient client;

        public DummyClientFixture(Type function, IDummyClient client)
        {

            this.client = client;
        }

        public async Task TestCrudOperations()
        {
            var dummy1 = new Dummy(null, "Key 1", "Content 1");
            var dummy2 = new Dummy(null, "Key 2", "Content 2");

            // Create one dummy
            var createdDummy1 = await this.client.CreateDummyAsync(null, dummy1);
            Assert.Equal(createdDummy1.Content, dummy1.Content);
            Assert.Equal(createdDummy1.Key, dummy1.Key);
            dummy1 = createdDummy1;

            // Create another dummy
            var createdDummy2 = await this.client.CreateDummyAsync(null, dummy2);
            Assert.Equal(createdDummy2.Content, dummy2.Content);
            Assert.Equal(createdDummy2.Key, dummy2.Key);
            dummy2 = createdDummy1;

            // Get all dummies
            var dummyDataPage = await this.client.GetDummiesAsync(
                null,
                new FilterParams(),
                new PagingParams(0, 5, false)
            );

            Assert.True(dummyDataPage.Data.Count == 2);

            // Update the dummy
            dummy1.Content = "Updated Content 1";
            var updatedDummy1 = await this.client.UpdateDummyAsync(null, dummy1);

            Assert.Equal(updatedDummy1.Content, dummy1.Content);
            Assert.Equal(updatedDummy1.Key, dummy1.Key);
            dummy1 = updatedDummy1;

            // Delete dummy
            await this.client.DeleteDummyAsync(null, dummy1.Id);

            // Try to get delete dummy
            var dummy = await this.client.GetDummyByIdAsync(null, dummy1.Id);
            Assert.Null(dummy);
        }
    }
}
