using System.Threading.Tasks;
using Xunit;

namespace PipServices3.Azure.Containers
{
    public class DummyCommandableAzureFunctionTest
    {
        private DummyAzureFunctionFixture fixture;

        public DummyCommandableAzureFunctionTest()
        {
            fixture = new DummyAzureFunctionFixture(CommandableFunctionContainer.Run);
        }

        [Fact]
        public async Task TestCrudOperations()
        {
            await fixture.TestCrudOperations();
        }
    }
}
