using Amazon.DynamoDBv2.DataModel;

namespace DnsFailoverManager
{
    [DynamoDBTable("DA_MONITOR_DNS")]
    class DaMonitorDNS
    {
        [DynamoDBHashKey]
        [DynamoDBProperty("TARGET_DNS")]
        public required string TargetDNS { get; set; }

        [DynamoDBProperty("STATUS")]
        public string? Status { get; set; }

        [DynamoDBProperty("FAILOVER_INSTANCE")]
        public string? FailOverInstance { get; set; }

        [DynamoDBProperty("FAILOVER_DNS_JSON")]
        public string? FailOverDNSJson { get; set; }

        [DynamoDBProperty("FAILBACK_DNS_JSON")]
        public string? FailBackDNSJson { get; set; }

        [DynamoDBProperty("STATUS_CHANGED_AT")]
        public string? StatusChangedAt { get; set; }
    }
}
