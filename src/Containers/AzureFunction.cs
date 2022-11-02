using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using PipServices3.Azure.Services;
using PipServices3.Azure.Utils;
using PipServices3.Commons.Config;
using PipServices3.Commons.Errors;
using PipServices3.Commons.Refer;
using PipServices3.Commons.Validate;
using PipServices3.Components.Count;
using PipServices3.Components.Log;
using PipServices3.Components.Trace;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using PipContainer = PipServices3.Container;

namespace PipServices3.Azure.Containers
{
    /// <summary>
    /// Abstract Azure Function, that acts as a container to instantiate and run components
    /// and expose them via external entry point.
    /// 
    /// When handling calls "cmd" parameter determines which what action shall be called, while
    /// other parameters are passed to the action itself.
    /// 
    /// Container configuration for this Azure Function is stored in <code>"./config/config.yml"</code> file.
    /// But this path can be overriden by <code>CONFIG_PATH</code> environment variable.
    /// 
    /// ### References ###
    ///     - *:logger:*:*:1.0                              (optional) <see cref="ILogger"/> components to pass log messages
    ///     - *:counters:*:*:1.0                            (optional) <see cref="ICounters"/> components to pass collected measurements
    ///     - *:service:azure-function:*:1.0                  (optional) <see cref="IAzureFunctionService"/> services to handle action requests
    ///     - *:service:commandable-azure-function:*:1.0      (optional) <see cref="IAzureFunctionService"/> services to handle action requests
    /// </summary>
    /// 
    /// <example>
    /// <code>
    /// 
    /// class MyAzureFunction : AzureFunction
    /// {
    ///     public MyAzureFunction() : base("mygroup", "MyGroup Azure Function")
    ///     {
    /// 
    ///     }
    /// }
    /// 
    /// ...
    /// 
    /// var AzureFunction = new MyAzureFunction();
    /// 
    /// await AzureFunction.RunAsync();
    /// Console.WriteLine("MyAzureFunction is started");
    /// 
    /// </code>
    /// </example>
    public abstract class AzureFunction: PipContainer.Container
    {
        private readonly ManualResetEvent _exitEvent = new ManualResetEvent(false);

        /// <summary>
        /// The performanc counters.
        /// </summary>
        protected CompositeCounters _counters = new CompositeCounters();

        /// <summary>
        /// The tracer.
        /// </summary>
        protected CompositeTracer _tracer = new CompositeTracer();

        /// <summary>
        /// The dependency resolver.
        /// </summary>
        protected DependencyResolver _dependencyResolver = new DependencyResolver();

        /// <summary>
        /// The map of registred validation schemas.
        /// </summary>
        protected Dictionary<string, Schema> _schemas = new();

        /// <summary>
        /// The map of registered actions.
        /// </summary>
        protected Dictionary<string, Func<HttpRequest, Task<IActionResult>>> _actions = new();

        /// <summary>
        /// The default path to config file.
        /// </summary>
        protected string _configPath = "../config/config.yml";

        /// <summary>
        /// Creates a new instance of this Azure Function function.
        /// </summary>
        /// <param name="name">(optional) a container name (accessible via ContextInfo)</param>
        /// <param name="descriptor">(optional) a container description (accessible via ContextInfo)</param>
        public AzureFunction(string name, string descriptor): base(name, descriptor)
        {
            _logger = new ConsoleLogger();
        }

        public AzureFunction() { }

        private string GetConfigPath()
        {
            return Environment.GetEnvironmentVariable("CONFIG_PATH") ?? this._configPath;
        }

        private ConfigParams GetParameters()
        {
            return ConfigParams.FromValue(Environment.GetEnvironmentVariables());
        }

        private void CaptureErrors(string correlationId)
        {
            AppDomain.CurrentDomain.UnhandledException += (obj, e) =>
            {
                _logger.Fatal(correlationId, e.ExceptionObject.ToString(), "Process is terminated");
                _exitEvent.Set();
            };
        }

        private void CaptureExit(string correlationId)
        {
            _logger.Info(correlationId, "Press Control-C to stop the microservice...");

            Console.CancelKeyPress += (sender, eventArgs) =>
            {
                _logger.Info(correlationId, "Goodbye!");

                eventArgs.Cancel = true;
                _exitEvent.Set();

                Environment.Exit(1);
            };

            // Wait and close
            _exitEvent.WaitOne();
        }

        /// <summary>
        /// Sets references to dependent components.
        /// </summary>
        /// <param name="references">references to locate the component dependencies. </param>
        public override void SetReferences(IReferences references)
        {
            base.SetReferences(references);
            _counters.SetReferences(references);
            _dependencyResolver.SetReferences(references);

            Register();
        }

        /// <summary>
        /// Opens the component.
        /// </summary>
        /// <param name="correlationId">(optional) transaction id to trace execution through call chain.</param>
        public new async Task OpenAsync(string correlationId)
        {
            if (IsOpen()) return;

            await base.OpenAsync(correlationId);
            RegisterServices();
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
        /// Runs this Azure Function, loads container configuration,
        /// instantiate components and manage their lifecycle,
        /// makes this function ready to access action calls.
        /// </summary>
        public async Task RunAsync()
        {
            var correlationId = _info.Name;
            var path = GetConfigPath();
            var parameters = GetParameters();
            ReadConfigFromFile(correlationId, path, parameters);

            CaptureErrors(correlationId);
            await OpenAsync(correlationId);
            CaptureExit(correlationId);
            await CloseAsync(correlationId);
        }

        /// <summary>
        /// Registers all actions in this Azure Function.
        /// 
        /// Note: Overloading of this method has been deprecated. Use <see cref="AzureFunctionService"/> instead.
        /// </summary>
        [Obsolete("Overloading of this method has been deprecated. Use AzureFunctionService instead.", false)]
        protected virtual void Register() { }

        /// <summary>
        /// Registers all Azure Function services in the container.
        /// </summary>
        protected void RegisterServices()
        {
            // Extract regular and commandable Azure Function services from references
            var services = this._references.GetOptional<IAzureFunctionService>(
                new Descriptor("*", "service", "azure-function", "*", "*")
            );
            var cmdServices = this._references.GetOptional<IAzureFunctionService>(
                new Descriptor("*", "service", "commandable-azure-function", "*", "*")
            );

            services.AddRange(cmdServices);

            // Register actions defined in those services
            foreach (var service in services)
            {

                var actions = service.GetActions();
                foreach (var action in actions)
                {
                    RegisterAction(action.Cmd, action.Schema, action.Action);
                }
            }
        }

        /// <summary>
        /// Registers an action in this Azure Function.
        /// 
        /// Note: This method has been deprecated. Use <see cref="AzureFunctionService"/> instead.
        /// </summary>
        /// <param name="cmd">a action/command name.</param>
        /// <param name="schema">a validation schema to validate received parameters.</param>
        /// <param name="action">an action function that is called when action is invoked.</param>
        /// <exception cref="UnknownException"></exception>
        [Obsolete("This method has been deprecated. Use AzureFunctionService instead.", false)]
        protected void RegisterAction(string cmd, Schema schema, Func<HttpRequest, Task<IActionResult>> action)
        {
            if (string.IsNullOrEmpty(cmd))
                throw new UnknownException(null, "NO_COMMAND", "Missing command");

            if (action == null)
                throw new UnknownException(null, "NO_ACTION", "Missing action");

            if (this._actions.ContainsKey(cmd))
                throw new UnknownException(null, "DUPLICATED_ACTION", cmd + "action already exists");

            Func<HttpRequest, Task<IActionResult>> actionCurl = async (req) =>
            {
                // Perform validation
                if (schema != null)
                {
                    var param = AzureFunctionContextHelper.GetParameters(req);
                    var correlationId = GetCorrelationId(req);
                    var err = schema.ValidateAndReturnException(correlationId, param, false);
                    if (err != null)
                        return AzureFunctionResponseSender.SendErrorAsync(err);
                }

                return await action(req);
            };

            _actions[cmd] = actionCurl;
        }

        /// <summary>
        /// Returns correlationId from Azure Function request.
        /// This method can be overloaded in child classes
        /// </summary>
        /// <param name="request">Azure Function request</param>
        /// <returns>Returns correlationId from request</returns>
        protected string GetCorrelationId(HttpRequest request)
        {
            return AzureFunctionContextHelper.GetCorrelationId(request);
        }

        /// <summary>
        /// Returns command from Azure Function request.
        /// This method can be overloaded in child classes
        /// </summary>
        /// <param name="request">Azure Function request</param>
        /// <returns>Returns command from request</returns>
        protected string GetCommand(HttpRequest request)
        {
            return AzureFunctionContextHelper.GetCommand(request);
        }

        /// <summary>
        /// Executes this Azure Function and returns the result.
        /// This method can be overloaded in child classes
        /// if they need to change the default behavior
        /// </summary>
        /// <param name="req">the request function</param>
        /// <returns>task</returns>
        /// <exception cref="BadRequestException"></exception>
        protected async Task<IActionResult> ExecuteAsync(HttpRequest req)
        {
            string cmd = GetCommand(req);
            string correlationId = GetCorrelationId(req);

            if (string.IsNullOrEmpty(cmd))
            {
                throw new BadRequestException(
                    correlationId,
                    "NO_COMMAND",
                    "Cmd parameter is missing"
                );
            }

            var action = this._actions[cmd];
            if (action == null)
            {
                throw new BadRequestException(
                    correlationId,
                    "NO_ACTION",
                    "Action " + cmd + " was not found"
                )
                .WithDetails("command", cmd);
            }

            return await action(req);
        }

        private async Task<IActionResult> Handler(HttpRequest req)
        {
            // If already started then execute
            if (IsOpen())
                return await ExecuteAsync(req);
            // Start before execute
            await RunAsync();
            return await ExecuteAsync(req);
        }

        /// <summary>
        /// Gets entry point into this Azure Function.
        /// </summary>
        /// <returns>Returns plugin function</returns>
        public Func<HttpRequest, Task<IActionResult>> GetHandler()
        {
            return Handler;
        }
    }
}