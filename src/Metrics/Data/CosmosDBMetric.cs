namespace PipServices3.Azure.Metrics.Data
{
    public enum CosmosDBMetric
    {
        Total_Requests,
        Http_2xx,
        Http_3xx,
        Http_400,
        Http_401,
        Throttled_Requests,
        Internal_Server_Error,
        Service_Unavailable,
        Average_Requests_per_Second,
        _Data_Size,
        Index_Size,
        Storage_Capacity,
        Available_Storage,
        Total_Request_Units,
        Document_Count,
        Observed_Read_Latency,
        Observed_Write_Latency,
        Max_RUs_Per_Second,
        Max_RUPM_Consumed_Per_Minute,
        Mongo_Query_Request_Charge,
        Mongo_Update_Request_Charge,
        Mongo_Delete_Request_Charge,
        Mongo_Insert_Request_Charge,
        Mongo_Count_Request_Charge_Aggregatio,
        Mongo_Other_Request_Charge,
        Mongo_Query_Request_Rate,
        Mongo_Update_Request_Rate,
        Mongo_Delete_Request_Rate,
        Mongo_Insert_Request_Rate,
        Mongo_Count_Request_Rate,
        Mongo_Other_Request_Rate,
        Mongo_Query_Failed_Requests,
        Mongo_Update_Failed_Requests,
        Mongo_Delete_Failed_Requests,
        Mongo_Insert_Failed_Requests,
        Mongo_Count_Failed_Requests,
        Mongo_Other_Failed_Requests,
        Service_Availability,
        Consistency_Level
    }
    public static class CosmosDBMetricExtensions
    {
        public static string ToMetricString(this CosmosDBMetric metricValue)
        {
            return metricValue.ToString().Replace('_',' ');
        }
    }
}
