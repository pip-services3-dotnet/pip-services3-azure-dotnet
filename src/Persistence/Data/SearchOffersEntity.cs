using System.Collections.Generic;
using System.Runtime.Serialization;

namespace PipServices3.Azure.Persistence.Data
{
    [DataContract]
    public class SearchOffersEntity
    {
        [DataMember(Name = "_rid")]
        public string ResourceId { get; set; }

        [DataMember(Name = "Offers")]
        public List<OfferEntity> Offers { get; set; } = new List<OfferEntity>();

        [DataMember(Name = "_count")]
        public int Count { get; set; }
    }
}
