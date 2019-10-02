using PipServices3.Azure.Metrics.Data;

using System;
using System.Linq;
using System.Text;

namespace PipServices3.Azure.Metrics
{
    public class QueryBuilder
    {
        const string DateTimeFormat = "yyy-MM-ddTHH:mm:ss.fffffffZ";
        private StringBuilder _filterBuilder = new StringBuilder();
        private bool _lastOperationOperator = false;

        private bool RequiresOperator()
        {
            if (_filterBuilder.Length == 0)
            {
                return false;
            }

            return !_lastOperationOperator;
        }

        public QueryBuilder And()
        {
            if (!RequiresOperator())
            {
                return this;
            }

            _filterBuilder.Append(" and ");
            _lastOperationOperator = true;
            return this;
        }

        public QueryBuilder Or()
        {
            if (!RequiresOperator())
            {
                return this;
            }

            _filterBuilder.Append(" or ");
            _lastOperationOperator = true;
            return this;
        }

        public QueryBuilder AddMetric(params string[] metricNames)
        {
            if (RequiresOperator())
            {
                throw new Exception("Operator 'and' or 'or' expected!");
            }

            var subQueries = metricNames.Select(m => $"name.value eq '{m}'");
            var metricQuery = String.Join(" or ", subQueries);
            _filterBuilder.Append($"({metricQuery})");
            _lastOperationOperator = false;

            return this;
        }
        public QueryBuilder AddMetric(params CosmosDBMetric[] metrics)
        {
            return AddMetric(metrics.Select(m => m.ToMetricString()).ToArray());
        }

        public QueryBuilder AddPeriod(TimeSpan period, string interval)
        {
            var start = DateTime.UtcNow.Subtract(period);
            var end = DateTime.UtcNow;

            return AddTime(start, end, interval);
        }
        public QueryBuilder AddPeriod(TimeSpan period, MetricInterval interval)
        {
            return AddPeriod(period, interval.ToString());
        }

        public QueryBuilder AddTime(DateTime utcFrom, DateTime utcTo, string interval)
        {
            if (RequiresOperator())
            {
                throw new Exception("Operator 'and' or 'or' expected!");
            }

            var start = utcFrom.ToString(DateTimeFormat);
            var end = utcTo.ToString(DateTimeFormat);
            _filterBuilder.Append($"startTime eq {start} and endTime eq {end} and timeGrain eq duration'{interval}'");
            _lastOperationOperator = false;

            return this;
        }
        public QueryBuilder AddTime(DateTime utcFrom, DateTime utcTo, MetricInterval interval)
        {
            return this.AddTime(utcFrom, utcTo, interval.ToString());
        }

        public override string ToString()
        {
            if (_filterBuilder.Length == 0)
            {
                return null;
            }

            return _filterBuilder.ToString();
        }
    }
}
