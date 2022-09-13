using PipServices3.Commons.Data;
using PipServices3.Commons.Refer;
using PipServices3.Commons.Validate;
using System.Collections.Generic;
using PipServices3.Azure.Utils;
using PipServices3.Commons.Convert;

using TypeCode = PipServices3.Commons.Convert.TypeCode;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace PipServices3.Azure.Services
{
    public class DummyAzureFunctionService : AzureFunctionService
    {
        private IDummyController _controller;
        private IDictionary<string, string> _headers = new Dictionary<string, string>() { { "Content-Type", "application/json" } };

        public DummyAzureFunctionService() : base("dummies")
        {
            _dependencyResolver.Put("controller", new Descriptor("pip-services-dummies", "controller", "default", "*", "*"));
        }

        public override void SetReferences(IReferences references)
        {
            base.SetReferences(references);
            _controller = _dependencyResolver.GetOneRequired<IDummyController>("controller");
        }

        private async Task<IActionResult> GetPageByFilterAsync(HttpRequest request)
        {
            var body = AzureFunctionContextHelper.GetBodyAsParameters(request);
            var page = await _controller.GetPageByFilterAsync(
                GetCorrelationId(request),
                FilterParams.FromString(body.GetAsNullableString("filter")),
                PagingParams.FromTuples(
                    "total", AzureFunctionContextHelper.ExtractFromQuery("total", request),
                    "skip", AzureFunctionContextHelper.ExtractFromQuery("skip", request),
                    "take", AzureFunctionContextHelper.ExtractFromQuery("take", request)
                )
            );

            SetHeaders(request);

            return AzureFunctionResponseSender.SendResultAsync(page);
        }

        private async Task<IActionResult> GetOneByIdAsync(HttpRequest request)
        {
            var body = AzureFunctionContextHelper.GetBodyAsParameters(request);
            var dummy = await this._controller.GetOneByIdAsync(
            GetCorrelationId(request),
                body.GetAsNullableString("dummy_id")
            );

            SetHeaders(request);

            if (dummy != null)
                return AzureFunctionResponseSender.SendResultAsync(dummy);
            else
                return AzureFunctionResponseSender.SendEmptyResultAsync();
        }

        private async Task<IActionResult> CreateAsync(HttpRequest request)
        {
            var body = AzureFunctionContextHelper.GetBodyAsParameters(request);
            var dummy = await _controller.CreateAsync(
                GetCorrelationId(request),
                JsonConverter.FromJson<Dummy>(JsonConverter.ToJson(body.GetAsObject("dummy")))
            );

            SetHeaders(request);

            return AzureFunctionResponseSender.SendCreatedResultAsync(dummy);
        }

        private async Task<IActionResult> UpdateAsync(HttpRequest request)
        {
            var body = AzureFunctionContextHelper.GetBodyAsParameters(request);
            var dummy = await this._controller.UpdateAsync(
                GetCorrelationId(request),
                JsonConverter.FromJson<Dummy>(JsonConverter.ToJson(body.GetAsObject("dummy")))
            );

            SetHeaders(request);

            return AzureFunctionResponseSender.SendCreatedResultAsync(dummy);
        }

        private async Task<IActionResult> DeleteByIdAsync(HttpRequest request)
        {
            var body = AzureFunctionContextHelper.GetBodyAsParameters(request);
            var dummy = await this._controller.DeleteByIdAsync(
            GetCorrelationId(request),
                body.GetAsNullableString("dummy_id")
            );

            SetHeaders(request);

            return AzureFunctionResponseSender.SendDeletedResultAsync(dummy);
        }

        protected override void Register()
        {
            RegisterAction("get_dummies", new ObjectSchema()
                .WithOptionalProperty("body",
                    new ObjectSchema()
                        .WithOptionalProperty("filter", new FilterParamsSchema())
                        .WithOptionalProperty("paging", new PagingParamsSchema())
                        .WithRequiredProperty("cmd", TypeCode.String)
                ),
                GetPageByFilterAsync
            );

            RegisterAction("get_dummy_by_id", new ObjectSchema()
                .WithRequiredProperty("body",
                    new ObjectSchema()
                        .WithRequiredProperty("dummy_id", TypeCode.String)
                        .WithRequiredProperty("cmd", TypeCode.String)
                ),
                GetOneByIdAsync
            );

            RegisterAction("create_dummy", new ObjectSchema()
                .WithRequiredProperty("body",
                    new ObjectSchema()
                        .WithRequiredProperty("dummy", new DummySchema())
                        .WithRequiredProperty("cmd", TypeCode.String)
                ),
                CreateAsync
            );

            RegisterAction("update_dummy", new ObjectSchema()
                .WithRequiredProperty("body",
                    new ObjectSchema()
                        .WithRequiredProperty("dummy", new DummySchema())
                        .WithRequiredProperty("cmd", TypeCode.String)
                ),
                UpdateAsync
            );

            RegisterAction("delete_dummy", new ObjectSchema()
                .WithRequiredProperty("body",
                    new ObjectSchema()
                        .WithRequiredProperty("dummy_id", TypeCode.String)
                        .WithRequiredProperty("cmd", TypeCode.String)
                ),
                DeleteByIdAsync
            );
        }

        private void SetHeaders(HttpRequest req)
        {
            foreach (var key in _headers.Keys)
                req.HttpContext.Response.Headers.Add(key, _headers[key]);
        }
    }
}