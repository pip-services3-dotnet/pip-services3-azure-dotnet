using PipServices3.Commons.Convert;

using System;

namespace PipServices3.Azure.Persistence
{
    public static class CosmosDbThroughputHelper
    {
        public const int MinimumThroughput = 400;
        public const int MaximumThroughput = 200000;
        public const int BufferThroughput = 100;

        public static int GetRecommendedThroughput(int partitionCount, double maximumRequestUnitValue)
        {
            if (partitionCount <= 0)
            {
                return MinimumThroughput;
            }

            if (maximumRequestUnitValue <= MinimumThroughput)
            {
                maximumRequestUnitValue = MinimumThroughput;
            }

            var result = Math.Ceiling(maximumRequestUnitValue / 100) * 100;

            result = result * partitionCount + BufferThroughput;

            if (result <= MinimumThroughput)
            {
                return MinimumThroughput;
            }
            else if (result >= MaximumThroughput)
            {
                return MaximumThroughput;
            }

            return IntegerConverter.ToInteger(result);
        }
    }
}
