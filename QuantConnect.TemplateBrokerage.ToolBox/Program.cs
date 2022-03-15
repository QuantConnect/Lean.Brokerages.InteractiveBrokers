/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

using QuantConnect.ToolBox;
using QuantConnect.Configuration;
using static QuantConnect.Configuration.ApplicationParser;

namespace QuantConnect.TemplateBrokerage.ToolBox
{
    static class Program
    {
        static void Main(string[] args)
        {
            var optionsObject = ToolboxArgumentParser.ParseArguments(args);
            if (optionsObject.Count == 0)
            {
                PrintMessageAndExit();
            }

            if (!optionsObject.TryGetValue("app", out var targetApp))
            {
                PrintMessageAndExit(1, "ERROR: --app value is required");
            }

            var targetAppName = targetApp.ToString();
            if (targetAppName.Contains("download") || targetAppName.Contains("dl"))
            {
                var downloader = new TemplateBrokerageDownloader();
            }
            else if (targetAppName.Contains("updater") || targetAppName.EndsWith("spu"))
            {
                new ExchangeInfoUpdater(new TemplateExchangeInfoDownloader())
                    .Run();
            }
            else
            {
                PrintMessageAndExit(1, "ERROR: Unrecognized --app value");
            }
        }
    }
}