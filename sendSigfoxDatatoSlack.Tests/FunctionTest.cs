using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Text.Json;

using Xunit;
using Amazon.Lambda.Core;
using Amazon.Lambda.TestUtilities;

using sendSigfoxDatatoSlack;
using System.Net.Http;

namespace sendSigfoxDatatoSlack.Tests
{
    public class FunctionTest
    {
        //[Fact]
        //public async void TestPostSlack()
        //{
        //    var function = new Function();
        //    var webHookUrl = "";
        //    Assert.Equal("Send message detail: test message", await function.PostSlack(webHookUrl,"test message"));
        //}

        [Theory]
        [InlineData("0", "1970-1-1 09:00:00")]
        [InlineData("946609199", "1999-12-31 11:59:59")]
        [InlineData("946684800", "2000-1-1 09:00:00")]
        [InlineData("1577836800", "2020-1-1 09:00:00")]
        public void TestConvertUnixTime(string unixTime, string ans)
        {
            var function = new Function();
            var convertedTime = function.convertUnixTime(long.Parse(unixTime));
            var dateTimeAns = DateTimeOffset.Parse(ans).ToOffset(TimeSpan.FromHours(9));

            Assert.Equal(dateTimeAns.ToString(), convertedTime.ToString());
        }
    }
}
