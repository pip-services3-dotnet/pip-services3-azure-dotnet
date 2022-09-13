using PipServices3.Commons.Validate;
using System;

namespace PipServices3.Azure
{
    public class DummySchema: ObjectSchema
    {
        public DummySchema()
        {
            WithOptionalProperty("id", TypeCode.String);
            WithRequiredProperty("key", TypeCode.String);
            WithOptionalProperty("content", TypeCode.String);
        }
    }
}