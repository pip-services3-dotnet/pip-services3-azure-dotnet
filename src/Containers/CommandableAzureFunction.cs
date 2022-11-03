using Microsoft.AspNetCore.Http;

using PipServices3.Azure.Utils;
using PipServices3.Commons.Commands;
using PipServices3.Commons.Run;

using System;

namespace PipServices3.Azure.Containers
{
    /// <summary>
    /// Abstract Azure Function function, that acts as a container to instantiate and run components
    /// and expose them via external entry point. All actions are automatically generated for commands
    /// defined in <see cref="ICommandable"/> components. Each command is exposed as an action defined by "cmd" parameter.
    /// 
    /// Container configuration for this Azure Function is stored in <code>"./config/config.yml"</code> file.
    /// But this path can be overridden by <code>CONFIG_PATH</code> environment variable.
    /// 
    /// Note: This component has been deprecated. Use AzureFunctionService instead.
    /// 
    /// ### References ###
    ///     - *:logger:*:*:1.0                              (optional) <see cref="ILogger"/> components to pass log messages
    ///     - *:counters:*:*:1.0                            (optional) <see cref="ICounters"/> components to pass collected measurements
    ///     - *:service:azure-function:*:1.0                  (optional) <see cref="AzureFunctionService"/> services to handle action requests
    ///     - *:service:commandable-azure-function:*:1.0      (optional) <see cref="AzureFunctionService"/> services to handle action requests
    /// 
    /// </summary>
    /// 
    /// <example>
    /// <code>
    /// 
    /// class MyAzureFunction : CommandableAzureFunction
    /// {
    ///     private IMyController _controller;
    ///     ...
    ///     public MyAzureFunction() : base("mygroup", "MyGroup AzureFunction")
    ///     {
    /// 
    ///         this._dependencyResolver.Put(
    ///             "controller",
    ///             new Descriptor("mygroup", "controller", "*", "*", "1.0")
    ///         );
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
    public abstract class CommandableAzureFunction : AzureFunction
    {
        /// <summary>
        /// Creates a new instance of this Azure Function.
        /// </summary>
        /// <param name="name">(optional) a container name (accessible via ContextInfo)</param>
        /// <param name="description">(optional) a container description (accessible via ContextInfo)</param>
        public CommandableAzureFunction(string name, string description) : base(name, description)
        {
            _dependencyResolver.Put("controller", "none");
        }

        /// <summary>
        /// Creates a new instance of this Azure Function.
        /// </summary>
        public CommandableAzureFunction(): this(null, null) { }

        /// <summary>
        /// Returns body from Azure Function request.
        /// This method can be overloaded in child classes
        /// </summary>
        /// <returns></returns>
        protected Parameters GetParameters(HttpRequest request)
        {
            return AzureFunctionContextHelper.GetBodyAsParameters(request);
        }

        private void RegisterCommandSet(CommandSet commandSet)
        {
            var commands = commandSet.Commands;

            for (var index = 0; index < commands.Count; index++)
            {
                var command = commands[index];

                RegisterAction(command.Name, null, async (context) => {
                    var correlationId = GetCorrelationId(context);
                    var args = GetParameters(context);

                    try
                    {
                        using var timing = this.Instrument(correlationId, _info.Name + '.' + command.Name);
                        var result = await command.ExecuteAsync(correlationId, args);
                        return AzureFunctionResponseSender.SendResultAsync(result);
                    }
                    catch (Exception ex)
                    {
                        InstrumentError(correlationId, _info.Name + '.' + command.Name, ex);
                        return AzureFunctionResponseSender.SendErrorAsync(ex);
                    }
                });
            }
        }

        /// <summary>
        /// Registers all actions in this Azure Function.
        /// </summary>
        [Obsolete("Overloading of this method has been deprecated. Use AzureFunctionService instead.", false)]
        protected override void Register()
        {
            ICommandable controller = this._dependencyResolver.GetOneRequired<ICommandable>("controller");
            var commandSet = controller.GetCommandSet();
            this.RegisterCommandSet(commandSet);
        }
    }
}