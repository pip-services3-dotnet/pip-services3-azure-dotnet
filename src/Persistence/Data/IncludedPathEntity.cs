using System.Collections.Generic;
using System.Runtime.Serialization;

namespace PipServices3.Azure.Persistence.Data
{
    [DataContract]
    public class IncludedPathEntity
    {
        [DataMember(Name = "path")]
        public string Path { get; set; }

        [DataMember(Name = "indexes")]
        public List<IndexEntity> Indexes { get; set; } = new List<IndexEntity>();
    }
}
