using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Text.Json.Nodes;
using System.Globalization;
using System;

public static class 
AppInsightsProxy{

    private static readonly HttpClient client = new();

    [FunctionName("AppInsightsProxy")]
    public static async Task<IActionResult> 
    Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "AppInsightsProxy/{appId}")] HttpRequest request, string appId, ILogger logger) {
      
        var apiKey = request.Headers["X-Api-Key"].FirstOrDefault();
        if (string.IsNullOrWhiteSpace(apiKey))
            return new BadRequestObjectResult("Missing X-Api-Key header");
        
        using var reader = new StreamReader(request.Body);
        var body = await reader.ReadToEndAsync();

        var batchSize = 5000; 
        if (int.TryParse(request.Headers["X-Take-Limit"].FirstOrDefault(), out var parsedLimit))
            batchSize = parsedLimit;

        if (string.IsNullOrWhiteSpace(body))
            return new BadRequestObjectResult("Empty request body");

        string query;
        try {
            var json = JsonDocument.Parse(body);
            query = json.RootElement.GetProperty("query").GetString();
        }
        catch {
            return new BadRequestObjectResult("Invalid JSON body or missing 'query' property");
        }

        var fromTime = ExtractStartTime(query);
        var toTime = ExtractEndTime(query);

        var response = new StringBuilder();
        response.Append("[");

        var hasWritten = false;

        while (true) {
            var paginatedQuery = $"{query} | where timestamp >= datetime('{fromTime:O}') and timestamp < datetime('{toTime:O}') | order by timestamp asc | take {batchSize}";
            var appInsightsResponse = await GetAppInsightsResponse(appId, apiKey, paginatedQuery);

            if (appInsightsResponse is not { columns: { Count: > 0 }, rows: { Count: > 0 } })
                break;

            foreach (var record in GetRecords(appInsightsResponse.columns, appInsightsResponse.rows)) {
                if (hasWritten)
                    response.Append(",\n");

                response.Append(JsonSerializer.Serialize(record));
                hasWritten = true;
            }

            if (appInsightsResponse.rows.Count < batchSize)
                break;

            var lastTimestamp = DateTime.Parse(appInsightsResponse.rows.Last()[0]?.ToString() ?? "").AddMilliseconds(1);
            fromTime = lastTimestamp;
        }

        var result = response.ToString();
        response.Append("\n]");
        return new ContentResult {
            Content = result,
            ContentType = "application/json",
            StatusCode = 200
        };
    }


    public static DateTime 
    ParseApplicationInsightsDateTime(this string dateTime) =>
        DateTime.ParseExact(
            dateTime,
            new[] {
                "yyyy-MM-ddTHH:mm:ssZ",
                "yyyy-MM-ddTHH:mm:ss.fZ",
                "yyyy-MM-ddTHH:mm:ss.ffZ",
                "yyyy-MM-ddTHH:mm:ss.fffZ",
                "yyyy-MM-ddTHH:mm:ss.ffffZ",
                "yyyy-MM-ddTHH:mm:ss.fffffZ",
                "yyyy-MM-ddTHH:mm:ss.ffffffZ",
                "yyyy-MM-ddTHH:mm:ss.fffffffZ"
            },
            CultureInfo.InvariantCulture,
            DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal);
            
    public static string 
    ToAirbyteIsoString(this DateTime dateTime) => dateTime.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.ffffff", CultureInfo.InvariantCulture) + "Z";
    
    record AppInsightsResponse(List<JsonNode> columns, List<JsonNode> rows);

    private static async Task<AppInsightsResponse?> 
    GetAppInsightsResponse(string appId, string apiKey, string query) {
        var url = $"https://api.applicationinsights.io/v1/apps/{appId}/query";
        var payload = new StringContent(JsonSerializer.Serialize(new { query }), Encoding.UTF8, "application/json");

        client.DefaultRequestHeaders.Clear();
        client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        client.DefaultRequestHeaders.Add("X-Api-Key", apiKey);

        var response = await client.PostAsync(url, payload);
        var content = await response.Content.ReadAsStringAsync();

        var jsonNode = JsonNode.Parse(content);
        var table = jsonNode?["tables"]?[0];
        var columns = table?["columns"]?.AsArray()?.ToList();
        var rows = table?["rows"]?.AsArray()?.ToList();

        if (columns == null || rows == null)
            return null;

        return new AppInsightsResponse(columns, rows);
    }

    private static IEnumerable<JsonObject> 
    GetRecords(List<JsonNode> columns, List<JsonNode> rows) {
        var columnNames = columns.Select(c => c?["name"]?.ToString()).ToList();

        foreach (var row in rows) {
            var record = new JsonObject();
            for (var i = 0; i < columnNames.Count; i++) {
                var value = row?[i];
                var columnName = columnNames[i] ?? $"col{i}";

                if (columnName == "timestamp" && value is JsonValue jsonValue && jsonValue.TryGetValue<string>(out var time)) {
                    try {
                        record[columnName] = time.ParseApplicationInsightsDateTime().ToAirbyteIsoString();
                    }
                    catch {
                        record[columnName] = time;
                    }
                }
                else {
                    record[columnName] = value is null ? null : JsonValue.Create(value.GetValue<object>());
                }
            }
            yield return record;
        }
    }

    private static DateTime 
    ExtractStartTime(string query) {
        var match = System.Text.RegularExpressions.Regex.Match(query, @"timestamp\s*>=\s*datetime\('([^']+)'\)");
        return match.Success && DateTime.TryParse(match.Groups[1].Value, out var dateTime) ? dateTime : DateTime.UtcNow.AddHours(-1);
    }

    private static DateTime
    ExtractEndTime(string query) {
        var match = System.Text.RegularExpressions.Regex.Match(query, @"timestamp\s*<\s*datetime\('([^']+)'\)");
        return match.Success && DateTime.TryParse(match.Groups[1].Value, out var dateTime) ? dateTime : DateTime.UtcNow;
    }
}