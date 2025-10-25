using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.EC2;
using Amazon.Lambda.Core;
using CloudFlareDns;
using CloudFlareDns.Objects.Record;
using DnsClientX;
using System.Text;
using System.Text.Json;

[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace DnsFailoverManager;

public class Function
{

    private static readonly AmazonDynamoDBClient dbClient = new(RegionEndpoint.APNortheast1);
    private static readonly AmazonEC2Client ec2Client = new(RegionEndpoint.APNortheast1);

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
            bool needUpdate = false;

            foreach (var monitor in monitorTargets)
            {
                Console.WriteLine($"--- Checking target: {monitor.TargetDNS} (Status={monitor.Status}) ---");

                bool healthy = await CheckHealthAsync(monitor.TargetDNS);
                Console.WriteLine($"Health check result for {monitor.TargetDNS}: {(healthy ? "HEALTHY" : "UNHEALTHY")}");

                if (monitor.Status == "1")
                {
                    // ���ݐ���
                    if (!healthy)
                    {
                        Console.WriteLine($"FAILOVER triggered for {monitor.TargetDNS}");
                        string failPlace = "EC2";
                        try
                        {
                            // �X�e�[�^�X�X�V
                            monitor.Status = "0";
                            monitor.StatusChangedAt = DateTime.Now.ToString("o");
                            batchWrite.AddPutItem(monitor);
                            needUpdate = true;
                            Console.WriteLine($"Status updated to 0 (failover) for {monitor.TargetDNS}");

                            // EC2�N��
                            Console.WriteLine($"Starting EC2 instance: {monitor.FailOverInstance}");
                            await ec2Client.StartInstancesAsync(new Amazon.EC2.Model.StartInstancesRequest
                            {
                                InstanceIds = [monitor.FailOverInstance]
                            });

                            // Cloudflare DNS�X�V
                            failPlace = "EDNS";
                            Console.WriteLine("Updating Cloudflare DNS for FAILOVER...");
                            await FlareSync(monitor.FailOverDNSJson ?? "");

                            // SNS�ʒm
                            await PostToSlackAsync($"Failover executed for {monitor.TargetDNS} at {DateTime.Now:o}");
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[ERROR] Failover processing failed: {ex.Message}");
                            await PostToSlackAsync($"[ERROR] Failover failed for {monitor.TargetDNS} as {failPlace}");
                        }
                    }
                    else
                    {
                        Console.WriteLine("Healthy: no action required.");
                    }
                }
                else
                {
                    // ���݃t�F�C���I�[�o�[��
                    if (healthy)
                    {
                        Console.WriteLine($"FAILBACK triggered for {monitor.TargetDNS}");
                        string failPlace = "DNS";
                        try
                        {
                            // �X�e�[�^�X�X�V
                            monitor.Status = "1";
                            monitor.StatusChangedAt = DateTime.Now.ToString("o");
                            batchWrite.AddPutItem(monitor);
                            needUpdate = true;
                            Console.WriteLine($"Status updated to 1 (normal) for {monitor.TargetDNS}");

                            // Cloudflare DNS�X�V
                            Console.WriteLine("Updating Cloudflare DNS for FAILBACK...");
                            await FlareSync(monitor.FailBackDNSJson ?? "");

                            // EC2�V���b�g�_�E��
                            failPlace = "EC2";
                            Console.WriteLine($"Stopping EC2 instance: {monitor.FailOverInstance}");
                            await ec2Client.StopInstancesAsync(new Amazon.EC2.Model.StopInstancesRequest
                            {
                                InstanceIds = [monitor.FailOverInstance]
                            });

                            // SNS�ʒm
                            await PostToSlackAsync($"Failback executed for {monitor.TargetDNS} at {DateTime.Now:o}");
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[ERROR] Failback processing failed: {ex.Message}");
                            await PostToSlackAsync($"[ERROR] Failback failed for {monitor.TargetDNS} as {failPlace}");
                        }
                    }
                    else
                    {
                        Console.WriteLine("Still unhealthy: continuing failover state.");
                    }
                }
            }

            if (needUpdate)
            {
                Console.WriteLine("Applying batch updates to DynamoDB...");
                await batchWrite.ExecuteAsync();
                Console.WriteLine("Batch updates completed.");
            }

            Console.WriteLine("=== Lambda FunctionHandler completed successfully ===");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[FATAL] Unhandled exception in FunctionHandler: {ex}");
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

    private async Task PostToSlackAsync(string message)
    {
        using HttpClient client = new();
        var webhookUrl = Environment.GetEnvironmentVariable("WEBHOOK");
        var payload = new { text = message };
        var json = JsonSerializer.Serialize(payload);
        var content = new StringContent(json, Encoding.UTF8, "application/json");
        _ = await client.PostAsync(webhookUrl, content);
    }
}
