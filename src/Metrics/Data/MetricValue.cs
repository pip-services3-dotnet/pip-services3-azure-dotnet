using System;
using System.Collections.Generic;
using System.Runtime.Serialization;

namespace PipServices3.Azure.Metrics.Data
{
    [DataContract]
    public class MetricValue
    {
        [DataMember(Name = "average")]
        public double? Average { get; set; }

        [DataMember(Name = "_count")]
        public long? Count { get; set; }

        [DataMember(Name = "last")]
        public double? Last { get; set; }

        [DataMember(Name = "maximum")]
        public double? Maximum { get; set; }

        [DataMember(Name = "minimum")]
        public double? Minimum { get; set; }

        [DataMember(Name = "properties")]
        public IDictionary<string, string> Properties { get; set; } = new Dictionary<string, string>();

        [DataMember(Name = "timestamp")]
        public DateTime Timestamp { get; set; }

        [DataMember(Name = "total")]
        public double? Total { get; set; }

        public double? P10 { get; set; }

        public double? P25 { get; set; }

        public double? P50 { get; set; }

        public double? P75 { get; set; }

        public double? P90 { get; set; }

        public double? P95 { get; set; }

        public double? P99 { get; set; }
    }
}
