using System.Collections.Generic;
using System.Runtime.Serialization;

namespace PipServices3.Azure.Persistence.Data
{
    [DataContract]
    public class IndexingPolicyEntity
    {
        [DataMember(Name = "automatic")]
        public bool Automatic { get; set; }

        [DataMember(Name = "indexingMode")]
        public string IndexingMode { get; set; }

        [DataMember(Name = "includedPaths")]
        public List<IncludedPathEntity> IncludedPaths { get; set; } = new List<IncludedPathEntity>();

        [DataMember(Name = "excludedPaths")]
        public List<ExcludedPathEntity> ExcludedPaths { get; set; } = new List<ExcludedPathEntity>();
    }
}

