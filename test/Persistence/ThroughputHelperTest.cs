using Xunit;

namespace PipServices3.Azure.Persistence
{
    public class ThroughputHelperTest
    {
        [Theory]
        [InlineData(0, 0, AbstractCosmosDbPersistenceThroughputMonitor.DefaultMinimumThroughput)]
        [InlineData(0, -1, AbstractCosmosDbPersistenceThroughputMonitor.DefaultMinimumThroughput)]
        [InlineData(-1, 0, AbstractCosmosDbPersistenceThroughputMonitor.DefaultMinimumThroughput)]
        [InlineData(-1, -1, AbstractCosmosDbPersistenceThroughputMonitor.DefaultMinimumThroughput)]
        [InlineData(1, 100, AbstractCosmosDbPersistenceThroughputMonitor.DefaultMinimumThroughput + CosmosDbThroughputHelper.BufferThroughput)]
        public void It_Should_Recommend_Minimum_Throughput_Value(int partitionCount, double maximumRequestUnitValue, int expectedThroughput)
        {
            // act
            var result = CosmosDbThroughputHelper.GetRecommendedThroughput(partitionCount, maximumRequestUnitValue,
                AbstractCosmosDbPersistenceThroughputMonitor.DefaultMinimumThroughput, AbstractCosmosDbPersistenceThroughputMonitor.DefaultMaximumThroughput, AbstractCosmosDbPersistenceThroughputMonitor.DefaultGrowthRate);

            // assert
            Assert.Equal(expectedThroughput, result);
        }

        [Theory]
        [InlineData(1, AbstractCosmosDbPersistenceThroughputMonitor.DefaultMaximumThroughput + 1, AbstractCosmosDbPersistenceThroughputMonitor.DefaultMaximumThroughput)]
        [InlineData(100, AbstractCosmosDbPersistenceThroughputMonitor.DefaultMaximumThroughput + 1, AbstractCosmosDbPersistenceThroughputMonitor.DefaultMaximumThroughput)]
        public void It_Should_Recommend_Maximum_Throughput_Value(int partitionCount, double maximumRequestUnitValue, int expectedThroughput)
        {
            // act
            var result = CosmosDbThroughputHelper.GetRecommendedThroughput(partitionCount, maximumRequestUnitValue,
                AbstractCosmosDbPersistenceThroughputMonitor.DefaultMinimumThroughput, AbstractCosmosDbPersistenceThroughputMonitor.DefaultMaximumThroughput, AbstractCosmosDbPersistenceThroughputMonitor.DefaultGrowthRate);

            // assert
            Assert.Equal(expectedThroughput, result);
        }

        [Theory]
        [InlineData(1, 0, AbstractCosmosDbPersistenceThroughputMonitor.DefaultMinimumThroughput + CosmosDbThroughputHelper.BufferThroughput)]
        [InlineData(1, 101, AbstractCosmosDbPersistenceThroughputMonitor.DefaultMinimumThroughput + CosmosDbThroughputHelper.BufferThroughput)]
        [InlineData(1, 400, 500)]  // (1 * 400) = 400  + 100 = 500
        [InlineData(1, 420, 600)]  // (1 * 500) = 500  + 100 = 600
        [InlineData(2, 0, 900)]    // (2 * 400) = 800  + 100 = 900
        [InlineData(2, 199, 900)]  // (2 * 400) = 800  + 100 = 900
        [InlineData(2, 420, 1100)] // (2 * 500) = 1000 + 100 = 1100
        [InlineData(2, 460, 1100)] // (2 * 500) = 1000 + 100 = 1100
        [InlineData(3, 430, 1600)] // (3 * 500) = 1500 + 100 = 1600
        [InlineData(3, 0, 1300)]   // (3 * 400) = 1200 + 100 = 1300
        [InlineData(3, 450, 1600)] // (3 * 500) = 1500 + 100 = 1600
        [InlineData(4, 437, 2100)] // (4 * 500) = 2000 + 100 = 2100
        [InlineData(4, 473, 2100)] // (4 * 500) = 2000 + 100 = 2100
        [InlineData(5, 501, 3100)] // (5 * 600) = 3000 + 100 = 3100
        [InlineData(5, 599, 3100)] // (5 * 600) = 3000 + 100 = 3100
        public void It_Should_Recommend_Correct_Throughput_Value(int partitionCount, double maximumRequestUnitValue, int expectedThroughput)
        {
            // act
            var result = CosmosDbThroughputHelper.GetRecommendedThroughput(partitionCount, maximumRequestUnitValue,
                AbstractCosmosDbPersistenceThroughputMonitor.DefaultMinimumThroughput, AbstractCosmosDbPersistenceThroughputMonitor.DefaultMaximumThroughput, AbstractCosmosDbPersistenceThroughputMonitor.DefaultGrowthRate);

            // assert
            Assert.Equal(expectedThroughput, result);
        }

        [Theory]
        [InlineData(1, 5000, 1.5, 7600)]        // (1 * 5000) = 5000 * 1.5 = 2500 + 100 = 7600
        [InlineData(1, 100, 1.5, AbstractCosmosDbPersistenceThroughputMonitor.DefaultMinimumThroughput + CosmosDbThroughputHelper.BufferThroughput)]
        [InlineData(2, 5000, 1.5, 15100)]       // (2 * 5000) = 10000 * 1.5 = 5000 + 100 = 15100
        [InlineData(5, 5000, 1.5, 37600)]       // (5 * 5000) = 25000 * 1.5 = 37500 + 100 = 37600
        [InlineData(10, 5000, 1.5, 75100)]
        [InlineData(10, 10000, 1.5, AbstractCosmosDbPersistenceThroughputMonitor.DefaultMaximumThroughput)]
        public void It_Should_Recommend_Correct_Throughput_Value_For_Growth_Rate(int partitionCount, double maximumRequestUnitValue, double growthRate, int expectedThroughput)
        {
            // act
            var result = CosmosDbThroughputHelper.GetRecommendedThroughput(partitionCount, maximumRequestUnitValue,
                AbstractCosmosDbPersistenceThroughputMonitor.DefaultMinimumThroughput, AbstractCosmosDbPersistenceThroughputMonitor.DefaultMaximumThroughput, growthRate);

            // assert
            Assert.Equal(expectedThroughput, result);
        }
    }
}
