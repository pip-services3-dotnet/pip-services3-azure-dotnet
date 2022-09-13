using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using PipServices3.Commons.Convert;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace PipServices3.Azure.Containers
{

    public class DummyAzureFunctionFixture
    {
        protected Func<HttpRequest, ILogger, Task<IActionResult>> _function;
        private readonly ILogger logger = NullLoggerFactory.Instance.CreateLogger("Null Logger");

        public DummyAzureFunctionFixture(Func<HttpRequest, ILogger, Task<IActionResult>> function)
        {
            _function = function;
        }

        public async Task TestCrudOperations()
        {
            var DUMMY1 = new Dummy(null, "Key 1", "Content 1");
            var DUMMY2 = new Dummy(null, "Key 2", "Content 2");

            // Create one dummy
            var body = new Dictionary<string, object>() {
                { "cmd", "create_dummy" },
                { "dummy", DUMMY1 }
            };

            Dummy dummy1 = await ExecuteRequest<Dummy>(body);

            Assert.Equal(dummy1.Content, DUMMY1.Content);
            Assert.Equal(dummy1.Key, DUMMY1.Key);

            // Create another dummy
            body = new Dictionary<string, object>() {
                 { "cmd", "create_dummy" },
                 { "dummy", DUMMY2 }
            };

            Dummy dummy2 = await ExecuteRequest<Dummy>(body);
            Assert.Equal(dummy2.Content, DUMMY2.Content);
            Assert.Equal(dummy2.Key, DUMMY2.Key);

            // Update the dummy
            dummy1.Content = "Updated Content 1";
            body = new Dictionary<string, object>() {
                { "cmd", "update_dummy" },
                { "dummy", dummy1 }
            };

            Dummy updatedDummy1 = await ExecuteRequest<Dummy>(body);
            Assert.Equal(updatedDummy1.Id, dummy1.Id);
            Assert.Equal(updatedDummy1.Content, dummy1.Content);
            Assert.Equal(updatedDummy1.Key, dummy1.Key);
            dummy1 = updatedDummy1;

            // Delete dummy
            body = new Dictionary<string, object>() {
                { "cmd", "delete_dummy" },
                { "dummy_id", dummy1.Id }
            };

            Dummy deleted = await ExecuteRequest<Dummy>(body);

            Assert.Equal(deleted.Id, dummy1.Id);
            Assert.Equal(deleted.Content, dummy1.Content);
            Assert.Equal(deleted.Key, dummy1.Key);

            // Try to get deleted dummy
            body = new Dictionary<string, object>() {
                { "cmd", "get_dummy_by_id" },
                { "dummy_id", dummy1.Id }
            };

            var textResponse = await ExecuteRequest<string>(body);

            Assert.True(string.IsNullOrEmpty(textResponse));

            // Failed validation
            body = new Dictionary<string, object>() {
                { "cmd", "create_dummy" },
                { "dummy", null }
            };

            textResponse = await ExecuteRequest<string>(body, true);

            Assert.Contains("INVALID_DATA", textResponse);
        }

        private async Task<T> ExecuteRequest<T>(Dictionary<string, object> data, bool errResponse = false)
        {
            var json = JsonConverter.ToJson(data);

            var context = new DefaultHttpContext();
            var request = context.Request;
            request.Body = new MemoryStream();

            request.Body.Write(Encoding.UTF8.GetBytes(json));
            request.Body.Seek(0, SeekOrigin.Begin);

            var response = (ObjectResult)await _function(request, logger);

            if (!errResponse)
                Assert.True(response.StatusCode < 400);

            return (T)response.Value; // TODO check return type
        }
    }
}
