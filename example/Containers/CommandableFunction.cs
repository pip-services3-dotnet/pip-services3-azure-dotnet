using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using PipServices3.Commons.Config;

namespace PipServices3.Azure.Containers
{
    public static class CommandableFunctionContainer
    {
        public static DummyAzureFunction _functionService;
        public static Func<HttpRequest, Task<IActionResult>> _handler;

        [FunctionName("CommandableFunctionContainer")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            var config = ConfigParams.FromTuples(
                "logger.descriptor", "pip-services:logger:console:default:1.0",
                "controller.descriptor", "pip-services-dummies:controller:default:default:1.0"
            );

            if (_handler == null)
            {
                _functionService = new DummyAzureFunction();
                _functionService.Configure(config);
                await _functionService.OpenAsync(null);

                _handler = _functionService.GetHandler();
            }

            return await _handler(req);
        }
    }
}
