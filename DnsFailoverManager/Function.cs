using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.EC2;
using Amazon.Lambda.Core;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using CloudFlareDns;
using CloudFlareDns.Objects.Record;
using DnsClientX;
using System.Text.Json;

[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace DnsFailoverManager;

public class Function
{

    private static readonly AmazonDynamoDBClient dbClient = new(RegionEndpoint.APNortheast1);
    private static readonly AmazonEC2Client ec2Client = new(RegionEndpoint.APNortheast1);
    private static readonly AmazonSimpleNotificationServiceClient snsClient = new(RegionEndpoint.APNortheast1);

    public async Task FunctionHandler(ILambdaContext context)
    {
        Console.WriteLine("=== Lambda FunctionHandler started ===");

        try
        {
            using var dbContext = new DynamoDBContext(dbClient);

            Console.WriteLine("Fetching monitor targets from DynamoDB...");
            var monitorTargets = await dbContext.ScanAsync<DaMonitorDNS>(new List<ScanCondition>()).GetRemainingAsync();
            Console.WriteLine($"Fetched {monitorTargets.Count} targets.");

            var batchWrite = dbContext.CreateBatchWrite<DaMonitorDNS>();

            foreach (var monitor in monitorTargets)
            {
                Console.WriteLine($"--- Checking target: {monitor.TargetDNS} (Status={monitor.Status}) ---");

                bool healthy = await CheckHealthAsync(monitor.TargetDNS);
                Console.WriteLine($"Health check result for {monitor.TargetDNS}: {(healthy ? "HEALTHY" : "UNHEALTHY")}");

                if (monitor.Status == "1")
                {
                    // 現在正常
                    if (!healthy)
                    {
                        Console.WriteLine($"FAILOVER triggered for {monitor.TargetDNS}");

                        try
                        {
                            // EC2起動
                            Console.WriteLine($"Starting EC2 instance: {monitor.FailOverInstance}");
                            await ec2Client.StartInstancesAsync(new Amazon.EC2.Model.StartInstancesRequest
                            {
                                InstanceIds = [monitor.FailOverInstance]
                            });

                            // Cloudflare DNS更新
                            Console.WriteLine("Updating Cloudflare DNS for FAILOVER...");
                            await FlareSync(monitor.FailOverDNSJson ?? "");

                            // SNS通知
                            await SendSnsAsync($"[FAILOVER] {monitor.TargetDNS} is unhealthy. Switched to standby system.",
                                               $"Failover executed for {monitor.TargetDNS} at {DateTime.Now:o}");

                            // ステータス更新
                            monitor.Status = "0";
                            monitor.StatusChangedAt = DateTime.Now.ToString("o");
                            batchWrite.AddPutItem(monitor);
                            Console.WriteLine($"Status updated to 0 (failover) for {monitor.TargetDNS}");
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[ERROR] Failover processing failed: {ex.Message}");
                            await SendSnsAsync($"[ERROR] Failover failed for {monitor.TargetDNS}", ex.ToString());
                        }
                    }
                    else
                    {
                        Console.WriteLine("Healthy: no action required.");
                    }
                }
                else
                {
                    // 現在フェイルオーバー中
                    if (healthy)
                    {
                        Console.WriteLine($"FAILBACK triggered for {monitor.TargetDNS}");

                        try
                        {
                            // Cloudflare DNS更新
                            Console.WriteLine("Updating Cloudflare DNS for FAILBACK...");
                            await FlareSync(monitor.FailBackDNSJson ?? "");

                            // EC2シャットダウン
                            Console.WriteLine($"Stopping EC2 instance: {monitor.FailOverInstance}");
                            await ec2Client.StopInstancesAsync(new Amazon.EC2.Model.StopInstancesRequest
                            {
                                InstanceIds = [monitor.FailOverInstance]
                            });

                            // SNS通知
                            await SendSnsAsync($"[FAILBACK] {monitor.TargetDNS} is healthy again. Restored to primary system.",
                                               $"Failback executed for {monitor.TargetDNS} at {DateTime.Now:o}");

                            // ステータス更新
                            monitor.Status = "1";
                            monitor.StatusChangedAt = DateTime.Now.ToString("o");
                            batchWrite.AddPutItem(monitor);
                            Console.WriteLine($"Status updated to 1 (normal) for {monitor.TargetDNS}");
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[ERROR] Failback processing failed: {ex.Message}");
                            await SendSnsAsync($"[ERROR] Failback failed for {monitor.TargetDNS}", ex.ToString());
                        }
                    }
                    else
                    {
                        Console.WriteLine("Still unhealthy: continuing failover state.");
                    }
                }
            }

            Console.WriteLine("Applying batch updates to DynamoDB...");
            await batchWrite.ExecuteAsync();
            Console.WriteLine("Batch updates completed.");

            Console.WriteLine("=== Lambda FunctionHandler completed successfully ===");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[FATAL] Unhandled exception in FunctionHandler: {ex}");
            await SendSnsAsync("[FATAL] Lambda unhandled exception", ex.ToString());
        }
    }

    private async Task<bool> CheckHealthAsync(string dns)
    {
        Console.WriteLine($"Performing health check for {dns}...");
        try
        {
            using var dnsClient = new ClientX(dns, DnsRequestFormat.DnsOverTLS);
            var response = await dnsClient.Resolve("example.com", DnsRecordType.A);

            if (!string.IsNullOrEmpty(response.Error))
            {
                Console.WriteLine($"Health check error: {response.Error}");
                return false;
            }

            Console.WriteLine("Health check passed.");
            return true;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[ERROR] Exception during health check for {dns}: {ex.Message}");
            return false;
        }
    }

    private async Task FlareSync(string changesJson)
    {
        Console.WriteLine("Starting Cloudflare DNS synchronization...");
        try
        {
            List<DnsChange>? changes = JsonSerializer.Deserialize<List<DnsChange>>(changesJson)?
                .OrderBy(c => c.Order)
                .ToList();

            if (changes == null || changes.Count == 0)
            {
                Console.WriteLine("No DNS changes to process.");
                return;
            }

            string xAuthKey = Environment.GetEnvironmentVariable("CLOUDFLARE_API_KEY") ?? "";
            string xAuthEmail = Environment.GetEnvironmentVariable("CLOUDFLARE_EMAIL") ?? "";
            string zoneIdentifier = Environment.GetEnvironmentVariable("CLOUDFLARE_ZONE_ID") ?? "";

            CloudFlareDnsClient cloudFlareDnsClient = new(xAuthKey, xAuthEmail, zoneIdentifier);

            foreach (DnsChange change in changes)
            {
                Console.WriteLine($"Processing DNS change: {JsonSerializer.Serialize(change)}");

                var records = await cloudFlareDnsClient.Record.Get();

                RecordType rt = change.Type.ToUpper() switch
                {
                    "A" => RecordType.A,
                    "AAAA" => RecordType.AAAA,
                    "CNAME" => RecordType.CNAME,
                    "TXT" => RecordType.TXT,
                    "MX" => RecordType.MX,
                    "SRV" => RecordType.SRV,
                    "NS" => RecordType.NS,
                    "PTR" => RecordType.PTR,
                    "CAA" => RecordType.CAA,
                    _ => throw new Exception($"Unsupported record type: {change.Type}"),
                };

                var r = records.FirstOrDefault(r => r.Name == change.FQDN && r.Type == rt);

                try
                {
                    switch (change.Action.ToLower())
                    {
                        case "upsert":
                            if (r != null)
                            {
                                Console.WriteLine($"Updating existing record: {r.Name}");
                                r.Content = change.Value;
                                r.Ttl = change.TTL;
                                await cloudFlareDnsClient.Record.Update(r);
                            }
                            else
                            {
                                Console.WriteLine($"Creating new record: {change.FQDN}");
                                await cloudFlareDnsClient.Record.Create(change.FQDN, change.Value, false, rt, change.TTL);
                            }
                            break;

                        case "delete":
                            if (r != null)
                            {
                                Console.WriteLine($"Deleting record: {r.Name}");
                                await cloudFlareDnsClient.Record.Delete(r.Id);
                            }
                            else
                            {
                                Console.WriteLine($"Record not found for deletion: {change.FQDN}");
                            }
                            break;

                        default:
                            Console.WriteLine($"Unknown action: {change.Action}");
                            break;
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[ERROR] DNS change failed for {change.FQDN}: {ex.Message}");
                }
            }

            Console.WriteLine("Cloudflare DNS synchronization completed.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[ERROR] Exception in FlareSync: {ex}");
        }
    }

    private async Task SendSnsAsync(string subject, string message)
    {
        try
        {
            string snspn = Environment.GetEnvironmentVariable("SNS_PN") ?? "";

            if (string.IsNullOrEmpty(snspn))
            {
                Console.WriteLine("SNS_PN is not set. Skipping SNS notification.");
                return;
            }

            Console.WriteLine($"Sending SNS notification: {subject}");
            await snsClient.PublishAsync(new PublishRequest
            {
                PhoneNumber = snspn,
                Message = message
            });
            Console.WriteLine("SNS notification sent successfully.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[ERROR] Failed to send SNS notification: {ex.Message}");
        }
    }
}
