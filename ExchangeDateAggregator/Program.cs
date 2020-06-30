using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;

namespace ExchangeDateAggregatorNew
{
    public class ExchangeTransaction
    {
        public DateTime Date { get; set; }

        public float Price { get; set; }

        public float Amount { get; set; }

        public override string ToString()
        {
            return $"{Date.ToString()};{Price};{Amount}";
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            var srcFilepath = @"C:\Users\weichslu\Documents\Privat\MA\btc-historical-data-src.txt";
            var outFilepath = @"C:\Users\weichslu\Documents\Privat\MA\btc-historical-data-out.txt";
            var aggregateBy = "h"; // use s for seconds, m for minutes, h for hours

            var aggregateByMilliseconds = 0;

            switch (aggregateBy)
            {
                case "s":
                    aggregateByMilliseconds = 1000;
                    break;
                case "m":
                    aggregateByMilliseconds = 1000 * 60;
                    break;
                case "h":
                    aggregateByMilliseconds = 1000 * 60 * 60;
                    break;
                default:
                    aggregateByMilliseconds = 1000;
                    break;
            }

            var exchangeTransactions = new List<ExchangeTransaction>();

            FileStream fileStream = new FileStream(srcFilepath, FileMode.Open);
            using (StreamReader reader = new StreamReader(fileStream))
            {
                string line;

                while ((line = reader.ReadLine()) != null)
                {
                    if (line.ToLower().Contains("nan")) continue;
                    var splitted = line.Split(',');
                    if (splitted.Length != 8) continue;

                    var date = UnixTimeStampToDateTime(Double.Parse(splitted[0].Replace(".", ","), NumberStyles.Float));

                    exchangeTransactions.Add(new ExchangeTransaction
                    {
                        Date = date,
                        Price = float.Parse(splitted[7].Replace(".", ","), NumberStyles.Float),
                        Amount = float.Parse(splitted[5].Replace(".", ","), NumberStyles.Float)
                    });
                }
            }


            exchangeTransactions = exchangeTransactions.OrderBy(t => t.Date).ToList();

            var tempAggregatedTransactions = new List<ExchangeTransaction>();
            var aggregatedTransactions = new List<string>();

            foreach (var exchangeTransaction in exchangeTransactions)
            {
                if (tempAggregatedTransactions.Any() && exchangeTransaction.Date > tempAggregatedTransactions.FirstOrDefault().Date.AddMilliseconds(aggregateByMilliseconds))
                {
                    aggregatedTransactions.Add(new ExchangeTransaction
                    {
                        Date = tempAggregatedTransactions[tempAggregatedTransactions.Count / 2].Date,
                        Price = GetMedian(tempAggregatedTransactions.Select(t => t.Price).ToArray()),
                        Amount = GetMedian(tempAggregatedTransactions.Select(t => t.Amount).ToArray())
                    }.ToString());
                    tempAggregatedTransactions.Clear();
                }

                tempAggregatedTransactions.Add(exchangeTransaction);
            }


            File.WriteAllLines(outFilepath, aggregatedTransactions.ToArray());
        }

        public static DateTime UnixTimeStampToDateTime(double unixTimeStamp)
        {
            // Unix timestamp is seconds past epoch
            System.DateTime dtDateTime = new DateTime(1970, 1, 1, 0, 0, 0, 0, System.DateTimeKind.Utc);
            dtDateTime = dtDateTime.AddSeconds(unixTimeStamp).ToLocalTime();
            return dtDateTime;
        }

        public static float GetMedian(float[] xs)
        {
            Array.Sort(xs);
            return xs[xs.Length / 2];
        }
    }
}
