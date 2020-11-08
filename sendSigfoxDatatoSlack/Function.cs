using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.Model;
using Amazon.Lambda.Core;
using Amazon.Lambda.DynamoDBEvents;
using Slack.Webhooks;
using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading.Tasks;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace sendSigfoxDatatoSlack
{
    public class Function
    {
        private static readonly AmazonDynamoDBClient DbClient = new AmazonDynamoDBClient(RegionEndpoint.APNortheast1);

        /// <summary>
        /// Convert data from DynamoDB and post it to Slack
        /// DynamoDBからのデータを変換し、Slackにpostする
        /// </summary>
        /// <param name="input"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public async Task<List<AttributeValue>> FunctionHandler(DynamoDBEvent dynamoDBEvent, ILambdaContext context)
        {
            // DynamoDBEventオブジェクトをログ出力
            context.Logger.LogLine(JsonSerializer.Serialize(dynamoDBEvent));

            // Sigfoxのデータが格納されている、DynamoDBのテーブルからデータを取得する
            // 取得するトリガは、DynamoDB Stream
            // データ形式: JSON
            var sigfoxdata = new List<AttributeValue>();
            string pushUnixTime = "", sensorData = "";

            using (var dbContext = new DynamoDBContext(DbClient))
            {
                foreach (var records in dynamoDBEvent.Records)
                {
                    if (records.EventName == OperationType.INSERT || records.EventName == OperationType.MODIFY)
                    {
                        var sigfoxDataItem = new SigfoxDataTableItem
                        {
                            // キーはKeys（パーティションキーのみの場合は1つ、ソートキーを含む場合は2つ持つ）
                            Device = records.Dynamodb.Keys["device"].S,
                            PostAt = records.Dynamodb.Keys["time"].S,

                            // 値はNewImageから取得する
                            Payload = records.Dynamodb.NewImage["payload"].M
                        };

                        foreach (KeyValuePair<string, AttributeValue> kvp in sigfoxDataItem.Payload)
                        {
                            sigfoxdata.Add(kvp.Value);

                            if (kvp.Key == "time")
                            {
                                pushUnixTime = kvp.Value.S;
                            }

                            if (kvp.Key == "data")
                            {
                                sensorData = kvp.Value.S;
                            }
                        }
                        ReturnItem(sigfoxDataItem.Payload);
                    }
                }
            }

            // DynamoDBから取得したJSONを、メッセージに変換する
            // タイムスタンプはUnixTimeなので、日本時間に変換する
            var postJapanTime = convertUnixTime(long.Parse(pushUnixTime));
            var slackMessage = ":thermometer: 現在の気温は" + Hex2float(sensorData.Substring(4, 8)).ToString() + "℃です " +
                        "(Sensor push time: " + postJapanTime + ")\n";

            // 環境変数からWebHookURLを取得
            var slackWebHookUrl = Environment.GetEnvironmentVariable("SlackWebHook");
            Console.WriteLine(slackWebHookUrl);

            // Slackにメッセージを送信する
            await PostSlack(slackWebHookUrl, slackMessage);

            return sigfoxdata;
        }

        /// <summary>
        /// Get Sigfox posted data from DynamoDB and convert UnixTime to JapanTime(UTC+9)
        /// Unix Timeを日本時間(UTC+9)に変換する
        /// </summary>
        /// <param name="unixTime"></param>
        /// <returns></returns>
        public DateTimeOffset convertUnixTime(long unixTime = 0)
        {
            DateTimeOffset time = DateTimeOffset.FromUnixTimeSeconds(unixTime).ToOffset(new TimeSpan(9, 0, 0));

            return time;
        }

        /// <summary>
        /// Post message to Slack
        /// Slackにポストする
        /// </summary>
        /// <param name="webHookUrl"></param>
        /// <param name="message"></param>
        /// <returns></returns>
        public async Task<string> PostSlack(string webHookUrl, string message)
        {
            var client = new SlackClient(webHookUrl);
            var slackMessage = new SlackMessage
            {
                Channel = "#sigfox_test",
                Text = message,
                IconEmoji = Emoji.RobotFace,
                Username = "Sigfox Bot"
            };

            await client.PostAsync(slackMessage);

            return "Send message detail: " + message ;
        }

        /// <summary>
        /// HEX value convet to float value
        /// デバイスデータをfloatデータに変換する
        /// </summary>
        /// <param name="hexStr"></param>
        /// <returns></returns>
        public static float Hex2float(string hexStr)
        {
            uint num = uint.Parse(hexStr, System.Globalization.NumberStyles.AllowHexSpecifier);
            byte[] floatVals = BitConverter.GetBytes(num);

            return _ = BitConverter.ToSingle(floatVals, 0);
        }

        /// <summary>
        /// Writes out an item's attribute keys and values.
        /// DynamoDBから取得したデータの内容のKeyごとの値を返す
        /// </summary>
        /// <param name="attrs"></param>
        public static void ReturnItem(Dictionary<string, AttributeValue> attrs)
        {
            foreach (KeyValuePair<string, AttributeValue> kvp in attrs)
            {
                Console.Write(kvp.Key + " = ");
                PrintValue(kvp.Value);
            }
        }

        /// <summary>
        /// Writes out just an attribute's value.
        /// </summary>
        /// <param name="value"></param>
        public static void PrintValue(AttributeValue value)
        {
            // Binary attribute value.
            if (value.B != null)
            {
                Console.Write("Binary data");
            }
            // Binary set attribute value.
            else if (value.BS.Count > 0)
            {
                foreach (var bValue in value.BS)
                {
                    Console.Write("\n  Binary data");
                }
            }
            // List attribute value.
            else if (value.L.Count > 0)
            {
                foreach (AttributeValue attr in value.L)
                {
                    PrintValue(attr);
                }
            }
            // Map attribute value.
            else if (value.M.Count > 0)
            {
                Console.Write("\n");
                ReturnItem(value.M);
            }
            // Number attribute value.
            else if (value.N != null)
            {
                Console.Write(value.N);
            }
            // Number set attribute value.
            else if (value.NS.Count > 0)
            {
                Console.Write("{0}", string.Join("\n", value.NS.ToArray()));
            }
            // Null attribute value.
            else if (value.NULL)
            {
                Console.Write("Null");
            }
            // String attribute value.
            else if (value.S != null)
            {
                Console.Write(value.S);
            }
            // String set attribute value.
            else if (value.SS.Count > 0)
            {
                Console.Write("{0}", string.Join("\n", value.SS.ToArray()));
            }
            // Otherwise, boolean value.
            else
            {
                Console.Write(value.BOOL);
            }

            Console.Write("\n");
        }
    }

    /// <summary>
    /// Sigfoxのデータモデル
    /// </summary>
    [DynamoDBTable("sigfoxdata")]
    internal class SigfoxDataTableItem
    {
        [DynamoDBHashKey]
        [DynamoDBProperty(attributeName:"device")]
        public string Device { get; set; }

        [DynamoDBProperty(attributeName:"time")]
        public string PostAt { get; set; }

        [DynamoDBProperty(attributeName:"payload")]
        public Dictionary<string, AttributeValue> Payload { get; set; }
    }
}
