using PipServices3.Azure.Containers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PipServices3.Azure.Services
{
    public class DummyAzureFunction: AzureFunction
    {
        public DummyAzureFunction() :base("dummy", "Dummy Azure function")
        {
            _factories.Add(new DummyFactory());
        }
    }
}