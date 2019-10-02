using System.Runtime.Serialization;

namespace PipServices3.Azure.Persistence.Data
{
    [DataContract]
    public class CollectionEntity
    {
        [DataMember(Name = "id")]
        public string Id { get; set; }

        [DataMember(Name = "_rid")]
        public string ResourceId { get; set; }

        [DataMember(Name = "indexingPolicy")]
        public IndexingPolicyEntity IndexingPolicy { get; set; }

        [DataMember(Name = "partitionKey")]
        public PartitionKeyEntity PartitionKey { get; set; }
    }
}
