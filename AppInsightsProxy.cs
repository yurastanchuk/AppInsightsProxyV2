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
    public static async Task<IActionResult> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "AppInsightsProxy/{appId}")] HttpRequest request, string appId, ILogger logger) {
      
        var apiKey = request.Headers["X-Api-Key"].FirstOrDefault();
        if (string.IsNullOrWhiteSpace(apiKey))
            return new BadRequestObjectResult("Missing X-Api-Key header");
        
        using var reader = new StreamReader(request.Body);
        var body = await reader.ReadToEndAsync();

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

        var url = $"https://api.applicationinsights.io/v1/apps/{appId}/query";

        client.DefaultRequestHeaders.Clear();
        client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        client.DefaultRequestHeaders.Add("X-Api-Key", apiKey);

        var payload = new StringContent(JsonSerializer.Serialize(new { query }), Encoding.UTF8, "application/json");

        var response = await client.PostAsync(url, payload);
        var content = await response.Content.ReadAsStringAsync();

        var jsonNode = JsonNode.Parse(content);
        var table = jsonNode?["tables"]?[0];
        var columns = table?["columns"]?.AsArray();
        var rows = table?["rows"]?.AsArray();

        if (columns == null || rows == null)
            return new BadRequestObjectResult("Missing columns or rows in Application Insights response");

        var columnNames = columns.Select(c => c?["name"]?.ToString()).ToList();
        var records = new List<JsonObject>();

        foreach (var row in rows) {
            var record = new JsonObject();
            for (var i = 0; i < columnNames.Count; i++) {
                var columnName = columnNames[i] ?? $"column{i}";
                var value = row?[i];

                if (columnName == "timestamp" && value is JsonValue jsonValue && jsonValue.TryGetValue<string>(out var timeStamp)) {
                    try {
                        record[columnName] = timeStamp.ParseApplicationInsightsDateTime().ToAirbyteIsoString();
                    }
                    catch {
                        record[columnName] = timeStamp; 
                    }
                }
                else {
                    record[columnName] = value is null ? null : JsonValue.Create(value.GetValue<object>());
                }
            }
            records.Add(record);
        }

        var resultJson = JsonSerializer.Serialize(records);
        return new ContentResult {
            Content = resultJson,
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
    

}