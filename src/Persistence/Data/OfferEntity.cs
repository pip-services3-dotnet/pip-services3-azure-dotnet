using System.Runtime.Serialization;

namespace PipServices3.Azure.Persistence.Data
{
    [DataContract]
    public class OfferEntity
    {
        [DataMember(Name = "id")]
        public string Id { get; set; }

        [DataMember(Name = "_rid")]
        public string ResourceId { get; set; }

        [DataMember(Name = "offerResourceId")]
        public string OfferResourceId { get; set; }

        [DataMember(Name = "resource")]
        public string Resource { get; set; }

        [DataMember(Name = "content")]
        public OfferContentEntity Content { get; set; }

        [DataMember(Name = "offerType")]
        public string OfferType { get; set; }

        [DataMember(Name = "offerVersion")]
        public string OfferVersion { get; set; }
    }
}
