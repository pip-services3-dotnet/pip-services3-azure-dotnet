using System.Runtime.Serialization;

namespace PipServices3.Azure.Persistence.Data
{
    [DataContract]
    public class ExcludedPathEntity
    {
        [DataMember(Name = "path")]
        public string Path { get; set; }
    }
}