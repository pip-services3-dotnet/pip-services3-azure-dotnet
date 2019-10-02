using Xunit;

namespace PipServices3.Azure.Persistence
{
    public class ThroughputHelperTest
    {
        [Theory]
        [InlineData(0, 0, CosmosDbThroughputHelper.MinimumThroughput)]
        [InlineData(0, -1, CosmosDbThroughputHelper.MinimumThroughput)]
        [InlineData(-1, 0, CosmosDbThroughputHelper.MinimumThroughput)]
        [InlineData(-1, -1, CosmosDbThroughputHelper.MinimumThroughput)]
        [InlineData(1, 100, CosmosDbThroughputHelper.MinimumThroughput + CosmosDbThroughputHelper.BufferThroughput)]
        public void It_Should_Recommend_Minimum_Throughput_Value(int partitionCount, double maximumRequestUnitValue, int expectedThroughput)
        {
            // act
            var result = CosmosDbThroughputHelper.GetRecommendedThroughput(partitionCount, maximumRequestUnitValue);

            // assert
            Assert.Equal(expectedThroughput, result);
        }

        [Theory]
        [InlineData(1, CosmosDbThroughputHelper.MaximumThroughput + 1, CosmosDbThroughputHelper.MaximumThroughput)]
        [InlineData(100, CosmosDbThroughputHelper.MaximumThroughput + 1, CosmosDbThroughputHelper.MaximumThroughput)]
        public void It_Should_Recommend_Maximum_Throughput_Value(int partitionCount, double maximumRequestUnitValue, int expectedThroughput)
        {
            // act
            var result = CosmosDbThroughputHelper.GetRecommendedThroughput(partitionCount, maximumRequestUnitValue);

            // assert
            Assert.Equal(expectedThroughput, result);
        }

        [Theory]
        [InlineData(1, 0, CosmosDbThroughputHelper.MinimumThroughput + CosmosDbThroughputHelper.BufferThroughput)]
        [InlineData(1, 101, CosmosDbThroughputHelper.MinimumThroughput + CosmosDbThroughputHelper.BufferThroughput)]
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
            var result = CosmosDbThroughputHelper.GetRecommendedThroughput(partitionCount, maximumRequestUnitValue);

            // assert
            Assert.Equal(expectedThroughput, result);
        }
    }

}
