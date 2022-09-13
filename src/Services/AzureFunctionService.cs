#if NETCOREAPP3_1_OR_GREATER

using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using PipServices3.Azure.Utils;
using PipServices3.Commons.Config;
using PipServices3.Commons.Refer;
using PipServices3.Commons.Run;
using PipServices3.Commons.Validate;
using PipServices3.Components.Count;
using PipServices3.Components.Log;
using PipServices3.Components.Trace;

namespace PipServices3.Azure.Services
{
    /// <summary>
    /// Abstract service that receives remove calls via Azure Function protocol.
    /// 
    /// This service is intended to work inside AzureFunction container that
    /// exposes registered actions externally.
    /// 
    /// ### Configuration parameters ###
    /// 
    /// - dependencies:
    ///     - controller:            override for Controller dependency
    ///     
    /// ### References ###
    ///     - *:logger:*:*:1.0               (optional) <see cref="ILogger"/> components to pass log messages
    ///     - *:counters:*:*:1.0             (optional) <see cref="ICounters"/> components to pass collected measurements
    /// </summary>
    /// 
    /// <example>
    /// <code>
    /// 
    /// public class MyAzureFunctionService : AzureFunctionService
    /// {
    ///     private IMyController _controller;
    /// 
    ///     public MyAzureFunctionService(IMyController controller) : base("v1.myservice")
    ///     {
    ///         _controller = controller;
    /// 
    ///         this._dependencyResolver.Put("controller", new Descriptor("mygroup", "controller", "*", "*", "1.0"));
    ///     }
    /// 
    ///     public override void SetReferences(IReferences references)
    ///     {
    ///         base.SetReferences(references);
    ///         _controller = _dependencyResolver.GetRequired<IMyController>("controller");
    ///     }
    /// 
    ///     public static void m()
    ///     {
    ///         var service = new MyAzureFunctionService(controller);
    ///         service.Configure(ConfigParams.FromTuples(
    ///             "connection.protocol", "http",
    ///             "connection.host", "localhost",
    ///             "connection.port", 8080
    ///         ));
    ///         service.SetReferences(References.FromTuples(
    ///            new Descriptor("mygroup", "controller", "default", "default", "1.0"), controller
    ///         ));
    /// 
    ///         await service.OpenAsync("123");
    ///     }
    /// 
    ///     protected override void Register()
    ///     {
    ///         RegisterAction("get_dummies", new ObjectSchema()
    ///             .WithOptionalProperty("body",
    ///                 new ObjectSchema()
    ///                     .WithOptionalProperty("filter", new FilterParamsSchema())
    ///                     .WithOptionalProperty("paging", new PagingParamsSchema())
    ///                     .WithRequiredProperty("cmd", TypeCode.String)
    ///             ),
    ///             async (req) =>
    ///             {
    ///                 var correlationId = GetCorrelationId(req);
    ///                 var body = AzureFunctionContextHelper.GetBodyAsParameters(req);
    ///                 var id = body.GetAsString("id");
    ///                 return await this._controller.getMyData(correlationId, id);
    ///             }
    ///         );
    ///     }
    /// }
    /// 
    /// 
    /// 
    /// </code>
    /// </example>
    public abstract class AzureFunctionService : IAzureFunctionService, IOpenable, IConfigurable,
    IReferenceable
    {

        private string _name;
        private List<AzureFunctionAction> _actions = new();
        private List<Func<HttpRequest, Func<HttpRequest, Task<IActionResult>>, Task<IActionResult>>> _interceptors = new();
        private bool _opened = false;

        /// <summary>
        /// The dependency resolver.
        /// </summary>
        protected DependencyResolver _dependencyResolver = new DependencyResolver();

        /// <summary>
        /// The logger.
        /// </summary>
        protected CompositeLogger _logger = new CompositeLogger();

        /// <summary>
        /// The performance counters.
        /// </summary>
        protected CompositeCounters _counters = new CompositeCounters();

        /// <summary>
        /// The tracer.
        /// </summary>
        protected CompositeTracer _tracer = new CompositeTracer();

        /// <summary>
        /// Creates an instance of this service.
        /// </summary>
        public AzureFunctionService(string name = null)
        {
            _name = name;
        }

        /// <summary>
        /// Creates an instance of this service.
        /// </summary>
        public AzureFunctionService()
        {
            _name = "";
        }

        /// <summary>
        /// Configures component by passing configuration parameters.
        /// </summary>
        /// <param name="config">configuration parameters to be set.</param>
        public void Configure(ConfigParams config)
        {
            _dependencyResolver.Configure(config);
        }

        /// <summary>
        /// Sets references to dependent components.
        /// </summary>
        /// <param name="references">references to locate the component dependencies. </param>
        public virtual void SetReferences(IReferences references)
        {
            _logger.SetReferences(references);
            _counters.SetReferences(references);
            _tracer.SetReferences(references);
            _dependencyResolver.SetReferences(references);
        }

        /// <summary>
        /// Adds instrumentation to log calls and measure call time. It returns a CounterTiming
        /// object that is used to end the time measurement.
        /// </summary>
        /// <param name="correlationId">(optional) transaction id to trace execution through call chain.</param>
        /// <param name="methodName">a method name.</param>
        /// <returns>CounterTiming object to end the time measurement.</returns>
        protected CounterTiming Instrument(string correlationId, string methodName)
        {
            _logger.Trace(correlationId, "Executing {0} method", methodName);
            _counters.IncrementOne(methodName + ".exec_count");
            return _counters.BeginTiming(methodName + ".exec_time");
        }

        /// <summary>
        /// Adds instrumentation to error handling.
        /// </summary>
        /// <param name="correlationId">(optional) transaction id to trace execution through call chain.</param>
        /// <param name="methodName">a method name.</param>
        /// <param name="ex">Error that occured during the method call</param>
        /// <param name="rethrow">True to throw the exception</param>
        protected void InstrumentError(string correlationId, string methodName, Exception ex, bool rethrow = false)
        {
            _logger.Error(correlationId, ex, "Failed to execute {0} method", methodName);
            _counters.IncrementOne(methodName + ".exec_errors");

            if (rethrow)
                throw ex;
        }

        /// <summary>
        /// Get all actions supported by the service.
        /// </summary>
        /// <returns>an array with supported actions.</returns>
        public IList<AzureFunctionAction> GetActions()
        {
            return _actions;
        }

        /// <summary>
        /// Checks if the component is opened.
        /// </summary>
        /// <returns>true if the component has been opened and false otherwise.</returns>
        public bool IsOpen()
        {
            return _opened;
        }

        /// <summary>
        /// Opens the component.
        /// </summary>
        /// <param name="correlationId">(optional) transaction id to trace execution through call chain.</param>
        public Task OpenAsync(string correlationId)
        {
            if (_opened)
                return Task.Delay(0);


            Register();

            _opened = true;

            return Task.Delay(0);
        }

        /// <summary>
        /// Closes component and frees used resources.
        /// </summary>
        /// <param name="correlationId">(optional) transaction id to trace execution through call chain.</param>
        public Task CloseAsync(string correlationId)
        {
            if (!_opened)
                return Task.Delay(0); ;

            _opened = false;
            _actions.Clear();
            _interceptors.Clear();

            return Task.Delay(0);
        }

        protected Func<HttpRequest, Task<IActionResult>> ApplyValidation(Schema schema, Func<HttpRequest, Task<IActionResult>> action)
        {
            // Create an action function
            async Task<IActionResult> actionWrapper(HttpRequest req)
            {
                // Validate object
                if (schema != null)
                {
                    // Perform validation
                    var parameters = AzureFunctionContextHelper.GetParameters(req);
                    var correlationId = this.GetCorrelationId(req);
                    var err = schema.ValidateAndReturnException(correlationId, parameters, false);

                    if (err != null)
                        return AzureFunctionResponseSender.SendErrorAsync(err);
                }

                return await action(req);
            };

            return actionWrapper;
        }

        protected Func<HttpRequest, Task<IActionResult>> ApplyInterceptors(Func<HttpRequest, Task<IActionResult>> action)
        {
            var actionWrapper = action;

            for (var index = _interceptors.Count - 1; index >= 0; index--)
            {
                var interceptor = _interceptors[index];

                Func<HttpRequest, Task<IActionResult>> wrapper(Func<HttpRequest, Task<IActionResult>> action)
                {
                    return async (HttpRequest req) => await interceptor(req, action);
                }

                actionWrapper = wrapper(actionWrapper);
            }

            return actionWrapper;
        }

        public string GenerateActionCmd(string name)
        {
            var cmd = name;
            if (_name != null)
                cmd = _name + "." + cmd;

            return cmd;
        }

        /// <summary>
        /// Registers a action in Azure Function function.
        /// </summary>
        /// <param name="name">an action name</param>
        /// <param name="schema">a validation schema to validate received parameters.</param>
        /// <param name="action">an action function that is called when operation is invoked.</param>
        protected void RegisterAction(string name, Schema schema, Func<HttpRequest, Task<IActionResult>> action)
        {
            var actionWrapper = ApplyValidation(schema, action);
            actionWrapper = ApplyInterceptors(action);

            var registeredAction = new AzureFunctionAction()
            {
                Cmd = GenerateActionCmd(name),
                Schema = schema,
                Action = async (req) => await actionWrapper(req)
            };

            _actions.Add(registeredAction);
        }

        /// <summary>
        /// Registers a action in Azure Function function with authorizer.
        /// </summary>
        /// <param name="name">an action name</param>
        /// <param name="schema">a validation schema to validate received parameters.</param>
        /// <param name="authorize">an action function that authorize user before calling action.</param>
        /// <param name="action">an action function that is called when operation is invoked.</param>
        protected void RegisterActionWithAuth(string name, Schema schema,
            Func<HttpRequest, Func<HttpRequest, Task<IActionResult>>, Task<IActionResult>> authorize,
            Func<HttpRequest, Task<IActionResult>> action)
        {
            var actionWrapper = this.ApplyValidation(schema, action);

            // Add authorization just before validation
            actionWrapper = (req) =>
            {
                return authorize(req, actionWrapper);
            };
            actionWrapper = this.ApplyInterceptors(actionWrapper);

            var self = this;
            var registeredAction = new AzureFunctionAction()
            {
                Cmd = GenerateActionCmd(name),
                Schema = schema,
                Action = async (req) => { return await actionWrapper(req); }
            };

            _actions.Add(registeredAction);
        }

        /// <summary>
        /// Registers a middleware for actions in Google Function service.
        /// </summary>
        /// <param name="action">an action function that is called when middleware is invoked.</param>
        protected void RegisterInterceptor(string cmd,
            Func<HttpRequest, Func<HttpRequest, Task<IActionResult>>, Task<IActionResult>> action)
        {
            async Task<IActionResult> interceptorWrapper(HttpRequest req, Func<HttpRequest, Task<IActionResult>> next)
            {
                var currCmd = GetCommand(req);
                var match = Regex.Match(currCmd, cmd, RegexOptions.IgnoreCase).Success;

                if (cmd != null && cmd != "" && !match)
                    return await next(req);
                else
                    return await action(req, next);
            }

            _interceptors.Add(interceptorWrapper);
        }

        /// <summary>
        /// Registers all service routes in HTTP endpoint.
        /// 
        /// This method is called by the service and must be overridden
        /// in child classes.
        /// </summary>
        protected abstract void Register();

        /// <summary>
        /// Returns command from Azure Function context.
        /// This method can be overloaded in child classes
        /// </summary>
        /// <param name="request">the context request</param>
        /// <returns>returns command from request/returns>
        protected string GetCommand(HttpRequest request)
        {
            return AzureFunctionContextHelper.GetCommand(request);
        }

        /// <summary>
        /// Returns correlationId from Azure Function context.
        /// This method can be overloaded in child classes
        /// </summary>
        /// <param name="request">the context request</param>
        /// <returns>returns correlationId from request</returns>
        protected string GetCorrelationId(HttpRequest request)
        {
            return AzureFunctionContextHelper.GetCorrelationId(request);
        }

        protected Parameters GetParameters(HttpRequest request)
        {
            return AzureFunctionContextHelper.GetBodyAsParameters(request);
        }
    }
}

#endif