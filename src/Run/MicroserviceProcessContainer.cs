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
            References = new ContainerReferences();
        }

        private void CaptureErrors()
        {
            AppDomain.CurrentDomain.UnhandledException += HandleUncaughtException;
        }

        private void HandleUncaughtException(object sender, UnhandledExceptionEventArgs args)
        {
            Logger.Fatal(_correlationId, (Exception)args.ExceptionObject, "Process is terminated");
        }

        private async Task RunAsync(string correlationId, CancellationToken token)
        {
            _correlationId = correlationId;

            CaptureErrors();

            //await StartAsync(correlationId, token);

            if (Config == null)
                throw new InvalidStateException(correlationId, "NO_CONFIG", "Container was not configured");

            try
            {
                Logger.Trace(correlationId, "Starting container.");

                // Create references with configured components
                InitReferences(References);
                References.PutFromConfig(Config);

                // Reference and open components
                var components = References.GetAll();
                Referencer.SetReferences(References, components);
                await Opener.OpenAsync(correlationId, References.GetAll());

                // Get reference to logger
                Logger = new CompositeLogger(References);

                // Get reference to container info
                var infoDescriptor = new Descriptor("*", "container-info", "*", "*", "*");
                Info = (ContainerInfo)References.GetOneRequired(infoDescriptor);

                Logger.Info(correlationId, "Container {0} started.", Info.Name);
            }
            catch (Exception ex)
            {
                References = null;
                Logger.Error(correlationId, ex, "Failed to start container");

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
            await StopAsync(_correlationId, token);
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
            ReadConfigFromFile(correlationId, path);
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

            return References.GetOneRequired<T>(new Descriptor("*", "service", "azure-stateless", "*", "*"));
        }
    }
}
