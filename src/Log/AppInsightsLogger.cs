using System;
using System.Collections.Generic;

using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.DataContracts;
using Microsoft.ApplicationInsights.Extensibility;

using PipServices3.Commons.Config;
using PipServices3.Components.Log;
using PipServices3.Components.Auth;

namespace PipServices3.Azure.Log
{
    /// <summary>
    /// Class AppInsightsLogger.
    /// </summary>
    /// <seealso cref="PipServices3.Commons.Log.Logger" />
    /// <seealso cref="PipServices3.Commons.Refer.IDescriptable" />
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
            if (_client == null)
            {
                Open();
            }

            if (Level < level)
            {
                return;
            }

            if (correlationId != null)
            {
                if (error != null)
                {
                    _client.TrackException(error, new Dictionary<string, string>
                    {
                        { "CorrelationId", correlationId },
                        { "message", message }
                    });
                }
                else
                {
                    _client.TrackTrace(message, LevelToSeverity(level), new Dictionary<string, string>
                    {
                        { "CorrelationId", correlationId }
                    });
                }
            }
            else
            {
                if (error != null)
                {
                    _client.TrackException(error);
                }
                else
                {
                    _client.TrackTrace(message, LevelToSeverity(level));
                }
            }
        }

        public void Dump()
        {
            _client.Flush();
        }
    }
}
