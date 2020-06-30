using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;

namespace BlockchainDataAggregator
{
    public class BcTransaction
    {
        public DateTime Timestamp { get; set; }

        public int BlockHeight { get; set; }

        public int SizeBytes { get; set; }

        public bool IsCoinbase { get; set; }

        public int Locktime { get; set; }

        public double Fee { get; set; }

        public int InputCount { get; set; }

        public double InputValue { get; set; }

        public int OutputCount { get; set; }

        public double OutputValue { get; set; }

        public double ChangeOutput { get; set; }

        public double OutputAdjusted
        {
            get
            {
                if (ChangeOutput == -1) return OutputValue;
                return OutputValue - ChangeOutput;
            }
        }

        public override string ToString()
        {
            return $"{Timestamp.ToString()};{Fee};{InputValue};{OutputValue};{ChangeOutput};{OutputAdjusted}";
        }
    }

    public class BcBlock
    {
        public DateTime Timestamp { get; set; }

        public int Height { get; set; }

        public int Version { get; set; }

        public double NBits { get; set; }

        public int TotalSize { get; set; }

        public int TxCount { get; set; }

        public double Fee { get; set; }

        public int InputCount { get; set; }

        public double InputValue { get; set; }

        public int OutputCount { get; set; }

        public double OutputValue { get; set; }

        public string Miner { get; set; }

        public double MinerRevenue { get; set; }

        public override string ToString()
        {
            return $"{Timestamp.ToString()};{Height};{Fee};{InputValue};{OutputValue};";
        }
    }

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

    public class AggregatedTransaction
    {
        public BcBlock Block { get; set; }

        public DateTime Date { get; set; }

        public double AvgPrice { get; set; }

        public double AvgAmountExchange { get; set; }

        public double AvgAmountBlockchain { get; set; }

        public double AvgFee { get; set; }

        public override string ToString()
        {
            return $"{Date.ToString()};{AvgPrice};{AvgAmountExchange};{AvgAmountBlockchain};{AvgFee}";
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            var aggregateBy = "m"; // use s for seconds, m for minutes, h for hours

            var aggregateByMilliseconds = 0;

            switch (aggregateBy)
            {
                case "s":
                    aggregateByMilliseconds = 1000;
                    break;
                case "10m":
                    aggregateByMilliseconds = 1000 * 60 * 10;
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

            var bcTransactions = GetBcTransactions(aggregateByMilliseconds).ToList();
            var bcBlocks = GetBcBlocks(aggregateByMilliseconds).ToList();
            var exTransactions = GetExTransactions(aggregateByMilliseconds);

            // get lowest date
            var currentDate = bcTransactions.FirstOrDefault().Timestamp;
            if (currentDate > exTransactions.FirstOrDefault().Date) currentDate = exTransactions.FirstOrDefault().Date;

            var tempAggregatedTransactions = new List<AggregatedTransaction>();
            var aggregatedTransactions = new List<AggregatedTransaction>();
            var tempBcTransactions = new List<BcTransaction>();
            var tempExTransactions = new List<ExchangeTransaction>();



            foreach (var transaction in bcTransactions)
            {
                if (tempBcTransactions.Any() && transaction.Timestamp > tempBcTransactions.FirstOrDefault().Timestamp.AddMilliseconds(aggregateByMilliseconds))
                {
                    aggregatedTransactions.Add(new AggregatedTransaction
                    {
                        Date = tempBcTransactions[tempBcTransactions.Count / 2].Timestamp,
                        AvgFee = Math.Round(tempBcTransactions.Select(t => t.Fee).Average()),
                        AvgAmountBlockchain = Math.Round(tempBcTransactions.Select(t => t.OutputAdjusted).Average())
                    });
                    tempBcTransactions.Clear();
                }

                tempBcTransactions.Add(transaction);
            }


            foreach (var transaction in exTransactions)
            {
                if (tempExTransactions.Any() && transaction.Date > tempExTransactions.FirstOrDefault().Date.AddMilliseconds(aggregateByMilliseconds))
                {
                    var exDate = tempExTransactions[tempExTransactions.Count / 2].Date;
                    var aggTx = aggregatedTransactions.FirstOrDefault(t => t.AvgPrice == 0 && t.Date < exDate);
                    if (aggTx == null)
                    {
                        tempExTransactions.Clear();
                        continue;
                    }
                    aggTx.AvgPrice = Math.Round(tempExTransactions.Select(t => t.Price).Average());
                    aggTx.AvgAmountExchange = Math.Round(tempExTransactions.Select(t => t.Amount).Average() * 100000000);

                    tempExTransactions.Clear();
                }

                tempExTransactions.Add(transaction);
            }

            var output = new List<string>();
            output.Add($"Date;AvgPrice;AvgAmountExchange;AvgAmountBlockchain;AvgFee");

            foreach (var tx in aggregatedTransactions)
            {
                if (tx.AvgPrice < 1)
                    continue;

                output.Add(tx.ToString());
            }

            File.WriteAllLines(@".\exchange-blockchain_aggregated.csv", output.ToArray());
        }

        public static DateTime UnixTimeStampToDateTime(double unixTimeStamp)
        {
            // Unix timestamp is seconds past epoch
            System.DateTime dtDateTime = new DateTime(1970, 1, 1, 0, 0, 0, 0, System.DateTimeKind.Utc);
            dtDateTime = dtDateTime.AddSeconds(unixTimeStamp).ToLocalTime();
            return dtDateTime;
        }

        public static IEnumerable<BcTransaction> GetBcTransactions(int aggregateByMilliseconds)
        {
            var dataSrcDir = @"C:\Users\weichslu\Documents\Privat\MA\src";

            var transactions = new List<BcTransaction>();

            foreach (var dataFile in Directory.GetFiles(dataSrcDir).OrderBy(f => f).ToList())
            {
                // skip if block
                if (!dataFile.Contains("tx")) continue;

                using (var reader = new StreamReader(new FileStream(dataFile, FileMode.Open)))
                {
                    string line;
                    var skipLine = true; //skip first line

                    while ((line = reader.ReadLine()) != null)
                    {
                        var splitted = line.Split(';');
                        if (splitted.Length != 11) continue;
                        if (skipLine)
                        {
                            skipLine = false;
                            continue;
                        }

                        // skip if coinbase
                        //if (string.IsNullOrEmpty(splitted[2]) || splitted[2] == "0") continue;

                        transactions.Add(new BcTransaction
                        {
                            Timestamp = UnixTimeStampToDateTime(Double.Parse(splitted[0].Replace(".", ","), NumberStyles.Float)),
                            BlockHeight = int.Parse(splitted[1].Replace(".", ","), NumberStyles.Integer),
                            SizeBytes = int.Parse(splitted[2].Replace(".", ","), NumberStyles.Integer),
                            IsCoinbase = bool.Parse(splitted[3].Replace(".", ",")),
                            Locktime = int.Parse(splitted[4].Replace(".", ","), NumberStyles.Integer),
                            Fee = double.Parse(splitted[5].Replace(".", ","), NumberStyles.Float),
                            InputCount = int.Parse(splitted[6].Replace(".", ","), NumberStyles.Integer),
                            InputValue = double.Parse(splitted[7].Replace(".", ","), NumberStyles.Float),
                            OutputCount = int.Parse(splitted[8].Replace(".", ","), NumberStyles.Integer),
                            OutputValue = double.Parse(splitted[9].Replace(".", ","), NumberStyles.Float),
                            ChangeOutput = double.Parse(splitted[10].Replace(".", ","), NumberStyles.Float)
                        });
                    }
                }
            }

            return transactions.OrderBy(t => t.Timestamp);
        }

        public static IEnumerable<BcBlock> GetBcBlocks(int aggregateByMilliseconds)
        {
            var dataSrcDir = @"C:\Users\weichslu\Documents\Privat\MA\src";

            var blocks = new List<BcBlock>();

            foreach (var dataFile in Directory.GetFiles(dataSrcDir).OrderBy(f => f).ToList())
            {
                // skip if transaction
                if (!dataFile.Contains("tx")) continue;

                using (var reader = new StreamReader(new FileStream(dataFile, FileMode.Open)))
                {
                    string line;
                    var skipLine = true; //skip first line

                    while ((line = reader.ReadLine()) != null)
                    {
                        var splitted = line.Split(';');
                        if (splitted.Length != 13) continue;
                        if (skipLine)
                        {
                            skipLine = false;
                            continue;
                        }

                        blocks.Add(new BcBlock()
                        {
                            Timestamp = UnixTimeStampToDateTime(Double.Parse(splitted[0].Replace(".", ","), NumberStyles.Float)),
                            Height = int.Parse(splitted[1].Replace(".", ","), NumberStyles.Integer),
                            Version = int.Parse(splitted[2].Replace(".", ","), NumberStyles.Integer),
                            NBits = double.Parse(splitted[3].Replace(".", ",")),
                            TotalSize = int.Parse(splitted[4].Replace(".", ","), NumberStyles.Integer),
                            TxCount = int.Parse(splitted[5].Replace(".", ","), NumberStyles.Integer),
                            Fee = double.Parse(splitted[6].Replace(".", ","), NumberStyles.Float),
                            InputCount = int.Parse(splitted[7].Replace(".", ","), NumberStyles.Integer),
                            InputValue = double.Parse(splitted[8].Replace(".", ","), NumberStyles.Float),
                            OutputCount = int.Parse(splitted[9].Replace(".", ","), NumberStyles.Integer),
                            OutputValue = double.Parse(splitted[10].Replace(".", ","), NumberStyles.Float),
                            Miner = splitted[11],
                            MinerRevenue = double.Parse(splitted[12].Replace(".", ","), NumberStyles.Float)
                        });
                    }
                }
            }

            return blocks.OrderBy(b => b.Height);
        }

        public static IEnumerable<ExchangeTransaction> GetExTransactions(int aggregateByMilliseconds)
        {
            var timestampFile = @".\UNIX.csv";
            var dataFile = @".\BTC.csv";


            var exchangeTransactions = new List<ExchangeTransaction>();

            using (var reader = new StreamReader(new FileStream(timestampFile, FileMode.Open)))
            {
                string line;

                while ((line = reader.ReadLine()) != null)
                {
                    var date = UnixTimeStampToDateTime(Double.Parse(line.Replace(".", ","), NumberStyles.Float));
                    exchangeTransactions.Add(new ExchangeTransaction
                    {
                        Date = date
                    });
                }
            }

            using (var reader = new StreamReader(new FileStream(dataFile, FileMode.Open)))
            {
                string line;
                int i = 0;

                while ((line = reader.ReadLine()) != null)
                {
                    var splitted = line.Split(',');
                    if (splitted.Length != 2) continue;

                    exchangeTransactions[i].Price = float.Parse(splitted[0].Replace(".", ","), NumberStyles.Float);
                    exchangeTransactions[i].Amount = float.Parse(splitted[1].Replace(".", ","), NumberStyles.Float);
                    i++;
                }

                if (i != exchangeTransactions.Count)
                {
                    Console.WriteLine($"i={i},transactions={exchangeTransactions.Count}");
                }
            }

            return exchangeTransactions.OrderBy(t => t.Date).ToList();
        }
    }
}
