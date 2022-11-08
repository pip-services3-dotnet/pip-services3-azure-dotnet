using PipServices3.Azure.Utils;
using PipServices3.Commons.Commands;
using System;

namespace PipServices3.Azure.Services
{
    /// <summary>
    /// Abstract service that receives commands via Azure Function protocol
    /// to operations automatically generated for commands defined in <see cref="ICommandable"/> components.
    /// Each command is exposed as invoke method that receives command name and parameters.
    /// 
    /// Commandable services require only 3 lines of code to implement a robust external
    /// Azure Function-based remote interface.
    /// 
    /// This service is intended to work inside Azure Function container that
    /// exploses registered actions externally.
    /// 
    /// ### Configuration parameters ###
    ///     - dependencies:
    ///         - controller:            override for Controller dependency
    ///         
    /// ### References ###
    /// 
    ///     - *:logger:*:*:1.0              (optional) <see cref="ILogger"/> components to pass log messages
    ///     - *:counters:*:*:1.0            (optional) <see cref="ICounters"/> components to pass collected measurements
    /// 
    /// See <see cref="AzureFunctionService"/>
    /// </summary>
    /// 
    /// <example>
    /// <code>
    /// 
    /// 
    /// class MyCommandableAzureFunctionService : CommandableAzureFunctionService
    /// {
    ///     private IMyController _controller;
    ///     // ...
    ///     public MyCommandableAzureFunctionService() : base()
    ///     {
    ///         this._dependencyResolver.Put(
    ///             "controller",
    ///             new Descriptor("mygroup", "controller", "*", "*", "1.0")
    ///         );
    ///     }
    /// }
    /// 
    /// ...
    /// 
    /// var service = new MyCommandableAzureFunctionService();
    /// service.SetReferences(References.FromTuples(
    ///    new Descriptor("mygroup", "controller", "default", "default", "1.0"), controller
    /// ));
    /// await service.OpenAsync("123");
    /// 
    /// Console.WriteLine("The Azure Function service is running");
    /// 
    /// 
    /// </code>
    /// </example>
    public abstract class CommandableAzureFunctionService: AzureFunctionService
    {
        private CommandSet _commandSet;

        /// <summary>
        /// Creates a new instance of the service.
        /// </summary>
        /// <param name="name">a service name.</param>
        public CommandableAzureFunctionService(string name): base(name)
        {   
            _dependencyResolver.Put("controller", "none");
        }

        /// <summary>
        /// Creates a new instance of the service.
        /// </summary>
        public CommandableAzureFunctionService() { }

        /// <summary>
        /// Registers all actions in Azure Function.
        /// </summary>
        protected override void Register()
        {
            ICommandable controller = _dependencyResolver.GetOneRequired<ICommandable>("controller");
            _commandSet = controller.GetCommandSet();

            var commands = this._commandSet.Commands;
            for (var index = 0; index < commands.Count; index++)
            {
                var command = commands[index];
                var name = command.Name;

                this.RegisterAction(name, null, async (request) => {
                    var correlationId = GetCorrelationId(request);
                    var args = GetParameters(request);
                    args.Remove("correlation_id");

                    try
                    {
                        using (var timing = Instrument(correlationId, name))
                        {
                            var result = await command.ExecuteAsync(correlationId, args);
                            return AzureFunctionResponseSender.SendResultAsync(result);
                        }
                    }
                    catch (Exception ex)
                    {
                        InstrumentError(correlationId, name, ex);
                        return AzureFunctionResponseSender.SendErrorAsync(ex);
                    }
                });
            }
        }
    }
}