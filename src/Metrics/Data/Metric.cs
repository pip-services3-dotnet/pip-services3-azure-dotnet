using System;
using System.Collections.Generic;
using System.Runtime.Serialization;

namespace PipServices3.Azure.Metrics.Data
{
    [DataContract]
    public class Metric
    {
        public LocalizableString DimensionName { get; set; }

        public LocalizableString DimensionValue { get; set; }

        [DataMember(Name = "endTime")]
        public DateTime EndTime { get; set; }

        [DataMember(Name = "metricValues")]
        public IList<MetricValue> MetricValues { get; set; }

        [DataMember(Name = "name")]
        public LocalizableString Name { get; set; }

        [DataMember(Name = "properties")]
        public IDictionary<string, string> Properties { get; set; } = new Dictionary<string, string>();

        [DataMember(Name = "resourceid")]
        public string ResourceId { get; set; }

        [DataMember(Name = "partitionId")]
        public string PartitionId { get; set; }

        [DataMember(Name = "partitionKeyRangeId")]
        public string PartitionKeyRangeId { get; set; }

        [DataMember(Name = "startTime")]
        public DateTime StartTime { get; set; }

        [DataMember(Name = "timeGrain")]
        public string TimeGrain { get; set; }

        [DataMember(Name = "unit")]
        public Unit Unit { get; set; }
    }
}
