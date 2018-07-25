using System;
using System.Collections.Generic;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.DataContracts;
using Microsoft.ApplicationInsights.Extensibility;
using PipServices.Commons.Config;
using PipServices.Components.Log;
using PipServices.Commons.Refer;
using PipServices.Components.Auth;
using PipServices.Commons.Convert;
using System.Text;

namespace PipServices.Azure.Log
{
    /// <summary>
    /// Class AppInsightsLogger.
    /// </summary>
    /// <seealso cref="PipServices.Commons.Log.Logger" />
    /// <seealso cref="PipServices.Commons.Refer.IDescriptable" />
    public class AppInsightsLogger : Logger
    {
        private CredentialResolver _credentialResolver = new CredentialResolver();
        private TelemetryClient _client;

        public override void Configure(ConfigParams config)
        {
            base.Configure(config);
            _credentialResolver.Configure(config, true);
        }

        private SeverityLevel LevelToSeverity(LogLevel level)
        {
            switch (level)
            {
                case LogLevel.Fatal:
                    return SeverityLevel.Critical;
                case LogLevel.Error:
                    return SeverityLevel.Error;
                case LogLevel.Warn:
                    return SeverityLevel.Warning;
                case LogLevel.Info:
                    return SeverityLevel.Information;
                case LogLevel.Debug:
                    return SeverityLevel.Verbose;
                case LogLevel.Trace:
                    return SeverityLevel.Verbose;
            }

            return SeverityLevel.Verbose;
        }

        private void Open()
        {
            var credential = _credentialResolver.LookupAsync("count").Result;

            var key = credential.AccessKey
                ?? credential.GetAsNullableString("instrumentation_key")
                ?? credential.GetAsNullableString("InstrumentationKey");

            if (key != null)
                TelemetryConfiguration.Active.InstrumentationKey = key;

            _client = new TelemetryClient();
        }

        protected override void Write(LogLevel level, string correlationId, Exception error, string message)
        {
            if (_client == null) Open();

            if (Level < level) return;

            var build = new StringBuilder();
            build.Append('[');
            build.Append(correlationId != null ? correlationId : "---");
            build.Append(':');
            build.Append(level.ToString());
            build.Append(':');
            build.Append(StringConverter.ToString(DateTime.UtcNow));
            build.Append("] ");

            build.Append(message);

            if (error != null)
            {
                if (message.Length == 0)
                    build.Append("Error: ");
                else
                    build.Append(": ");

                build.Append(ComposeError(error));
            }

            var output = build.ToString();

            if (correlationId != null)
            {
                var props = new Dictionary<string, string> { { "CorrelationId", correlationId } };

                if (error != null)
                    _client.TrackException(error, props);
                else
                    _client.TrackTrace(message, LevelToSeverity(level), props);
            }
            else
            {
                if (error != null)
                    _client.TrackException(error);
                else
                    _client.TrackTrace(message, LevelToSeverity(level));
            }
        }

        public void Dump()
        {
            _client.Flush();
        }
    }
}
