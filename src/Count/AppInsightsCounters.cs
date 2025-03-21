using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.Extensibility;

using PipServices3.Components.Auth;
using PipServices3.Commons.Config;
using PipServices3.Components.Count;
using PipServices3.Components.Connect;

using System.Collections.Generic;

namespace PipServices3.Azure.Count
{
    public class AppInsightsCounters : CachedCounters
    {
        private CredentialResolver _credentialResolver = new CredentialResolver();
        private ConnectionResolver _connectionResolver = new ConnectionResolver();
        private TelemetryClient _client;

        public AppInsightsCounters() { }

        public override void Configure(ConfigParams config)
        {
            base.Configure(config);
            _connectionResolver.Configure(config, true);
            _credentialResolver.Configure(config, true);
        }

        private void Open()
        {
            var credential = _credentialResolver.LookupAsync("count").Result;
            var connection = _connectionResolver.ResolveAsync("count").Result;

            var key = credential.AccessKey
                ?? credential.GetAsNullableString("instrumentation_key")
                 ?? credential.GetAsNullableString("InstrumentationKey");

            var config = TelemetryConfiguration.CreateDefault();

            if (!string.IsNullOrWhiteSpace(connection.Uri))
            {
                config.ConnectionString = connection.Uri;
            }
            else if (key != null)
            {
                config.InstrumentationKey = key;
            }

            _client = new TelemetryClient(config);
        }

        protected override void Save(IEnumerable<Counter> counters)
        {
            if (_client == null) Open();

            foreach (var counter in counters)
            {
                switch (counter.Type)
                {
                    case CounterType.Increment:
                        _client.TrackMetric(counter.Name, counter.Count.Value);
                        break;
                    case CounterType.Interval:
                        _client.TrackMetric(counter.Name, counter.Average.Value);
                        break;
                    case CounterType.LastValue:
                        _client.TrackMetric(counter.Name, counter.Last.Value);
                        break;
                    case CounterType.Statistics:
                        _client.TrackMetric(counter.Name, counter.Average.Value);
                        break;
                }
            }

            _client.Flush();
        }
    }
}
