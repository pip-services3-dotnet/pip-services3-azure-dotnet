using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using PipServices3.Commons.Validate;
using System;
using System.Threading.Tasks;

namespace PipServices3.Azure.Services
{
    public class AzureFunctionAction
    {
        /// <summary>
        /// Command to call the action
        /// </summary>
        public string Cmd { get; set; }

        /// <summary>
        /// Schema to validate action parameters
        /// </summary>
        public Schema Schema { get; set; }

        /// <summary>
        /// Action to be executed
        /// </summary>
        public Func<HttpRequest, Task<IActionResult>> Action { get; set; }
    }
}