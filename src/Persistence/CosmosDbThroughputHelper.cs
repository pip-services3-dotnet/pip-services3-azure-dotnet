using PipServices3.Commons.Convert;

using System;

namespace PipServices3.Azure.Persistence
{
    public static class CosmosDbThroughputHelper
    {
        public const int BufferThroughput = 100;

        public static int GetRecommendedThroughput(int partitionCount, double maximumRequestUnitValue, int minimumThroughput, int maximumThroughput, double growthRate)
        {
            if (partitionCount <= 0)
            {
                return minimumThroughput;
            }

            if (maximumRequestUnitValue <= minimumThroughput)
            {
                maximumRequestUnitValue = minimumThroughput;
                growthRate = 1.0;
            }

            if (growthRate <= 0)
            {
                growthRate = 1.0;
            }

            var result = Math.Ceiling(maximumRequestUnitValue / 100 * growthRate) * 100;

            result = result * partitionCount + BufferThroughput;

            if (result <= minimumThroughput)
            {
                return minimumThroughput;
            }
            else if (result >= maximumThroughput)
            {
                return maximumThroughput;
            }

            return IntegerConverter.ToInteger(result);
        }
    }
}
