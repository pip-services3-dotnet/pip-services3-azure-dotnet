using PipServices3.Commons.Config;
using PipServices3.Commons.Refer;
using PipServices3.Components.Config;

using Xunit;

namespace PipServices3.Azure.Lock
{
    public class CloudStorageTableLockTest
    {
        private readonly CloudStorageTableLock _lock;
        private readonly LockFixture _fixture;

        public CloudStorageTableLockTest()
        {
            var config = YamlConfigReader.ReadConfig(null, "..\\..\\..\\..\\config\\test_connections.yaml", null);
            var connectionString = config.GetAsString("storage_lock");
            
            _lock = new CloudStorageTableLock();

            _lock.Configure(ConfigParams.FromString(connectionString));
            _lock.SetReferences(new References());

            _fixture = new LockFixture(_lock);
        }

        [Fact(Skip = "Not valid credentials")]
        //[Fact]
        public void TestAcquireLock()
        {
            _fixture.TestAcquireLock();
        }

        [Fact(Skip = "Not valid credentials")]
        //[Fact]
        public void TestTryAcquireLock()
        {
            _fixture.TestTryAcquireLock();
        }

        [Fact(Skip = "Not valid credentials")]
        //[Fact]
        public void TestReleaseLock()
        {
            _fixture.TestReleaseLock();
        }
    }
}
