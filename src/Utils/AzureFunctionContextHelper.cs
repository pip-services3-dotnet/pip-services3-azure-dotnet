using PipServices3.Commons.Convert;
using PipServices3.Commons.Run;

using System.IO;
using System.Threading.Tasks;
using Microsoft.Extensions.Primitives;
using Microsoft.AspNetCore.Http;

namespace PipServices3.Azure.Utils
{
    /// <summary>
    /// Class that helps to prepare function requests
    /// </summary>
    public class AzureFunctionContextHelper
    {
        /// <summary>
        /// Returns correlationId from Azure Function context.
        /// </summary>
        /// <param name="request">the Azure Function request</param>
        /// <returns>returns correlationId from request</returns>
        public static string GetCorrelationId(HttpRequest request)
        {
            return GetPropertyByName(request, "correlation_id");
        }

        /// <summary>
        /// Returns command from Azure Function context.
        /// </summary>
        /// <param name="request">the Azure Function request</param>
        /// <returns>returns command from request</returns>
        public static string GetCommand(HttpRequest request)
        {
            return GetPropertyByName(request, "cmd");
        }

        /// <summary>
        /// Extracts parameter from query by name
        /// </summary>
        /// <param name="parameter">parameter name</param>
        /// <param name="request">the Azure Function request</param>
        /// <returns>parameter value or empty string</returns>
        public static string ExtractFromQuery(string parameter, HttpRequest request)
        {
            return request.Query.TryGetValue(parameter, out StringValues sortValues)
                ? sortValues.ToString()
                : string.Empty;
        }

        /// <summary>
        /// Get body as Parameters object from request
        /// </summary>
        /// <param name="request">the Azure Function request</param>
        /// <returns>Parameters object</returns>
        public static Parameters GetBodyAsParameters(HttpRequest request)
        {
            string body = ReadBody(request);

            return string.IsNullOrEmpty(body) ? new Parameters() : Parameters.FromJson(body);
        }

        /// <summary>
        /// Get request as Parameters object from request
        /// </summary>
        /// <param name="request">the Azure Function request</param>
        /// <returns>Parameters object</returns>
        public static Parameters GetParameters(HttpRequest request)
        {
            string body = ReadBody(request);

            body = "{ \"body\":" + body + " }";

            var parameters = string.IsNullOrEmpty(body)
                ? new Parameters() : Parameters.FromJson(body);

            foreach(var pair in request.Query)
                parameters.Set(pair.Key, pair.Value[0]);

            return parameters;
        }

        /// <summary>
        /// Read body from request
        /// </summary>
        /// <param name="request">the Azure Function request</param>
        /// <returns>body represents as string</returns>
        public static string ReadBody(HttpRequest request)
        {
            var body = string.Empty;

            // Keep the original stream in a separate
            // variable to restore it later if necessary.
            var stream = request.Body;

            try
            {
                using (var buffer = new MemoryStream())
                {
                    // Copy the request stream to the memory stream.
                    stream.CopyTo(buffer);

                    // Rewind the memory stream.
                    buffer.Position = 0L;

                    // Replace the request stream by the memory stream.
                    request.Body = buffer;

                    using (var streamReader = new StreamReader(request.Body))
                    {
                        body = streamReader.ReadToEnd();
                    }
                }
            }
            finally
            {
                // Restore the original stream.
                request.Body = stream;
                request.Body.Position = 0L;
            }

            return body;
        }

        /// <summary>
        /// Extract property from request by name
        /// </summary>
        /// <param name="request">Azure Function request object</param>
        /// <param name="name">parameter name</param>
        /// <returns>parameter value as string or null</returns>
        public static string GetPropertyByName(HttpRequest request, string name)
        {
            var param = request.Query.TryGetValue(name, out StringValues correlationId)
                ? correlationId.ToString()
                : null;

            if (string.IsNullOrEmpty(param))
            {
                
                var body = ReadBody(request);

                if (!string.IsNullOrEmpty(body))
                {
                    var bodyMap = JsonConverter.ToMap(body);
                    bodyMap.TryGetValue(name, out object paramObj);

                    param = paramObj != null ? paramObj.ToString() : null;
                }
            }

            return param;
        }
    }
}