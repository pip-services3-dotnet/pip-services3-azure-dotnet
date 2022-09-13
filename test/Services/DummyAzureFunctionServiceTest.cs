using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using PipServices3.Commons.Convert;
using Microsoft.AspNetCore.Http;
using System.IO;
using Microsoft.AspNetCore.Mvc;

namespace PipServices3.Azure.Services
{
    public class DummyAzureFunctionServiceTest
    {
        private DummyAzureFunctionFixture fixture;

        public DummyAzureFunctionServiceTest()
        {
            fixture = new DummyAzureFunctionFixture(Function.Run);
        }

        [Fact]
        public async Task TestCrudOperations()
        {
            await fixture.TestCrudOperations();
        }
    }
}
