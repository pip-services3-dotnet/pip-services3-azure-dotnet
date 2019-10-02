using System.Collections.Generic;
using System.Runtime.Serialization;

namespace PipServices3.Azure.Persistence.Data
{
    [DataContract]
    public class PartitionKeyEntity
    {
        [DataMember(Name = "paths")]
        public List<string> Paths { get; set; } = new List<string>();

        [DataMember(Name = "kind")]
        public string Kind { get; set; }

        [DataMember(Name = "version")]
        public int Version { get; set; }
    }
}