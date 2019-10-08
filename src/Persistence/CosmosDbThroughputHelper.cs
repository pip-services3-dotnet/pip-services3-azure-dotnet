using PipServices3.Commons.Convert;

using System;

namespace PipServices3.Azure.Persistence
{
    public static class CosmosDbThroughputHelper
    {
        public const int BufferThroughput = 100;

        public static int GetRecommendedThroughput(int partitionCount, double maximumRequestUnitValue, int minimumThroughput, int maximumThroughput)
        {
            if (partitionCount <= 0)
            {
                return minimumThroughput;
            }

            if (maximumRequestUnitValue <= minimumThroughput)
            {
                maximumRequestUnitValue = minimumThroughput;
            }

            var result = Math.Ceiling(maximumRequestUnitValue / 100) * 100;

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
