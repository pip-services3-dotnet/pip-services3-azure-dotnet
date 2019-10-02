using System.Runtime.Serialization;

namespace PipServices3.Azure.Persistence.Data
{
    [DataContract]
    public class IndexEntity
    {
        [DataMember(Name = "kind")]
        public string Kind { get; set; }

        [DataMember(Name = "dataType")]
        public string DataType { get; set; }

        [DataMember(Name = "precision")]
        public int Precision { get; set; }
    }
}
