using System.Threading.Tasks;
using Xunit;

namespace PipServices3.Azure.Services
{
    public class DummyCommandableAzureFunctionServiceTest
    {
        private DummyAzureFunctionFixture fixture;

        public DummyCommandableAzureFunctionServiceTest()
        {
            fixture = new DummyAzureFunctionFixture(CommandableFunction.Run);
        }

        [Fact]
        public async Task TestCrudOperations()
        {
            await fixture.TestCrudOperations();
        }
    }
}
