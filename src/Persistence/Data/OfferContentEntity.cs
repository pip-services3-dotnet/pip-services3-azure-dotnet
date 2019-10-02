using System.Runtime.Serialization;

namespace PipServices3.Azure.Persistence.Data
{
    [DataContract]
    public class OfferContentEntity
    {
        [DataMember(Name = "offerThroughput")]
        public int OfferThroughput { get; set; }

        [DataMember(Name = "userSpecifiedThroughput")]
        public int UserSpecifiedThroughput { get; set; }

        [DataMember(Name = "offerIsRUPerMinuteThroughputEnabled")]
        public string OfferIsRUPerMinuteThroughputEnabled { get; set; }

        [DataMember(Name = "offerIsAutoScaleEnabled")]
        public string OfferIsAutoScaleEnabled { get; set; }
    }
}
