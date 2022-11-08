using Microsoft.AspNetCore.Mvc;
using PipServices3.Commons.Convert;
using PipServices3.Commons.Errors;
using System;
using System.Net;

namespace PipServices3.Azure.Utils
{
    /// <summary>
    /// Helper class that send responses
    /// </summary>
    public class AzureFunctionResponseSender
    {
        /// <summary>
        /// Sends error serialized as ErrorDescription object and appropriate HTTP status
        /// code.If status code is not defined, it uses 500 status code.
        /// </summary>
        /// <param name="response">a Http response</param>
        /// <param name="ex">an error object to be sent.</param>
        public static IActionResult SendErrorAsync(Exception ex)
        {
            var response = new ObjectResult(null);
            response.ContentTypes.Add("application/json");

            // Unwrap exception
            if (ex is AggregateException)
            {
                var ex2 = ex as AggregateException;
                ex = ex2.InnerExceptions.Count > 0 ? ex2.InnerExceptions[0] : ex;
            }

            if (ex is Commons.Errors.ApplicationException)
            {
                var ex3 = ex as Commons.Errors.ApplicationException;
                response.StatusCode = ex3.Status;
                var contentResult = JsonConverter.ToJson(ErrorDescriptionFactory.Create(ex3));
                response.Value = contentResult;
            }
            else
            {
                response.StatusCode = (int)HttpStatusCode.InternalServerError;
                response.Value = ErrorDescriptionFactory.Create(ex);
            }

            return response;
        }

        /// <summary>
        /// Creates a callback function that sends result as JSON object. That callack
        /// function call be called directly or passed as a parameter to business logic components.
        /// 
        /// If object is not null it returns 200 status code. For null results it returns
        /// 204 status code. If error occur it sends ErrorDescription with approproate status code.
        /// </summary>
        /// <param name="response">a Http response</param>
        /// <param name="result">a body object to result.</param>
        public static IActionResult SendResultAsync(object result)
        {
            var response = new ObjectResult(null);

            if (result == null)
                response.StatusCode = (int)HttpStatusCode.NoContent;
            else
            {
                response.ContentTypes.Add("application/json");
                response.StatusCode = (int)HttpStatusCode.OK;
                response.Value = result;
            }

            return response;
        }

        /// <summary>
        /// Sends an empty result with 204 status code.
        /// If error occur it sends ErrorDescription with approproate status code.
        /// </summary>
        /// <param name="response">aHttp response</param>
        public static IActionResult SendEmptyResultAsync()
        {
            var response = new ObjectResult(null);

            response.ContentTypes.Add("application/json");
            response.StatusCode = (int)HttpStatusCode.NoContent;

            return response;
        }

        /// <summary>
        /// Sends newly created object as JSON. That
        /// callack function call be called directly or passed as a parameter to business logic components.
        /// 
        /// If object is not null it returns 201 status code. For null results it returns
        /// 204 status code. If error occur it sends ErrorDescription with approproate status code.
        /// </summary>
        /// <param name="response">a Http response</param>
        /// <param name="result">a body object to created result</param>
        public static IActionResult SendCreatedResultAsync(object result)
        {
            var response = new ObjectResult(null);

            if (result == null)
                response.StatusCode = (int)HttpStatusCode.NoContent;
            else
            {
                response.ContentTypes.Add("application/json");
                response.StatusCode = (int)HttpStatusCode.Created;
                response.Value = result;
            }

            return response;
        }

        /// <summary>
        /// Creates a callback function that sends deleted object as JSON. That callack
        /// function call be called directly or passed as a parameter to business logic components.
        /// 
        /// If object is not null it returns 200 status code. For null results it returns
        /// 204 status code. If error occur it sends ErrorDescription with approproate status code.
        /// </summary>
        /// <param name="response">a Http response</param>
        /// <param name="result">a body object to deleted result</param>
        public static IActionResult SendDeletedResultAsync(object result)
        {
            var response = new ObjectResult(null);

            if (result == null)
                response.StatusCode = (int)HttpStatusCode.NoContent;
            else
            {
                response.ContentTypes.Add("application/json");
                response.StatusCode = (int)HttpStatusCode.OK;
                response.Value = result;
            }

            return response;
        }
    }
}