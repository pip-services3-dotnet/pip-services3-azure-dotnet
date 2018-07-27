using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Services.Runtime;
using PipServices.Components.Build;
using PipServices.Commons.Errors;
using PipServices.Components.Log;
using PipServices.Commons.Refer;
using PipServices.Commons.Run;
using PipServices.Components.Info;
using PipServices.Container.Refer;

namespace PipServices.Azure.Run
{
    /// <summary>
    /// Class MicroserviceProcessContainer.
    /// </summary>
    public class MicroserviceProcessContainer : Container.Container
    {
        private string _correlationId;
        private IFactory _factory;

        /// <summary>
        /// Initializes a new instance of the <see cref="MicroserviceProcessContainer"/> class.
        /// </summary>
        public MicroserviceProcessContainer()
        {
            _references = new ContainerReferences();
        }

        private void CaptureErrors()
        {
            AppDomain.CurrentDomain.UnhandledException += HandleUncaughtException;
        }

        private void HandleUncaughtException(object sender, UnhandledExceptionEventArgs args)
        {
            _logger.Fatal(_correlationId, (Exception)args.ExceptionObject, "Process is terminated");
        }

        private async Task RunAsync(string correlationId, CancellationToken token)
        {
            _correlationId = correlationId;

            CaptureErrors();

            //await StartAsync(correlationId, token);

            if (_config == null)
                throw new InvalidStateException(correlationId, "NO_CONFIG", "Container was not configured");

            try
            {
                _logger.Trace(correlationId, "Starting container.");

                // Create references with configured components
                InitReferences(_references);
                _references.PutFromConfig(_config);

                // Reference and open components
                var components = _references.GetAll();
                Referencer.SetReferences(_references, components);
                await Opener.OpenAsync(correlationId, _references.GetAll());

                // Get reference to logger
                _logger = new CompositeLogger(_references);

                // Get reference to container info
                var infoDescriptor = new Descriptor("*", "container-info", "*", "*", "*");
                _info = (ContextInfo)_references.GetOneRequired(infoDescriptor);

                _logger.Info(correlationId, "Container {0} started.", _info.Name);
            }
            catch (Exception ex)
            {
                _references = null;
                _logger.Error(correlationId, ex, "Failed to start container");

                throw;
            }
        }

        /// <summary>
        /// stop as an asynchronous operation.
        /// </summary>
        /// <param name="token">The token.</param>
        /// <returns>Task.</returns>
        public async Task StopAsync(CancellationToken token)
        {
            await StopAsync(token);
        }


        /// <summary>
        /// Runs the with configuration file asynchronous.
        /// </summary>
        /// <param name="correlationId">The correlation identifier.</param>
        /// <param name="path">The path.</param>
        /// <param name="token">The token.</param>
        /// <returns>Task.</returns>
        public Task RunWithConfigFileAsync(string correlationId, string path, CancellationToken token)
        {
            ReadConfigFromFile(correlationId, path, null);
            return RunAsync(correlationId, token);
        }

        /// <summary>
        /// Gets stateless service.
        /// </summary>
        /// <returns>StatelessService.</returns>
        public T GetService<T>()
            where T: class
        {
            if ( !typeof(StatelessService).IsAssignableFrom(typeof(T)) && !typeof(StatefulService).IsAssignableFrom(typeof(T)))
                throw new ArgumentException("Service should be derived from either StatelessService or StatefulService", nameof(T));

            return _references.GetOneRequired<T>(new Descriptor("*", "service", "azure-stateless", "*", "*"));
        }
    }
}
