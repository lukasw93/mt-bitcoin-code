using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace BlockchainExchangeAggregator
{
    public class ExchangeTransaction
    {
        public string Name { get; set; }

        public DateTime Timestamp { get; set; }

        public double Price { get; set; }

        public double Amount { get; set; }

        public override string ToString()
        {
            return $"{Name}: {Timestamp.ToString()},{Price},{Amount}";
        }
    }

    public class BlockchainBlock
    {
        public string Name { get; set; }

        public DateTime Timestamp { get; set; }

        public double MiningTimeMinutes { get; set; }

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

        public double FeeShareOnRevenue => Fee / MinerRevenue;

        public override string ToString()
        {
            return $"{Name}: {Height}";
        }

        /// <summary>
        /// Writes the data of this object to a csv string
        /// </summary>
        /// <param name="s"></param>
        /// <returns></returns>
        public string ToCsvHeader(string s = ";")
        {
            var propNames = "";

            foreach (var prop in this.GetType().GetProperties())
            {
                if (!string.IsNullOrEmpty(propNames)) propNames += s;
                propNames += prop.Name;
            }

            return propNames;
        }

        /// <summary>
        /// Writes the data of this object to a csv string
        /// </summary>
        /// <param name="s"></param>
        /// <returns></returns>
        public string ToCsvData(string s = ";")
        {
            var propValues = "";

            foreach (var prop in this.GetType().GetProperties())
            {
                if (!string.IsNullOrEmpty(propValues)) propValues += s;
                propValues += this.GetType().GetProperty(prop.Name).GetValue(this);
            }

            return propValues;
        }
    }

    public class BlockchainTransaction
    {
        public string Name { get; set; }

        public DateTime Timestamp { get; set; }

        public int BlockHeight { get; set; }

        public int SizeBytes { get; set; }

        public bool IsCoinbase { get; set; }

        public double Locktime { get; set; }

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
            return $"{Name}: {Timestamp.ToString()};{Fee};{InputValue};{OutputValue};{ChangeOutput};{OutputAdjusted}";
        }
    }

    public class AggregatedExchangeTransaction
    {
        public string ExchangeName { get; set; }
        public double PriceOpen { get; set; }
        public double PriceHigh { get; set; }
        public double PriceLow { get; set; }
        public double PriceClose { get; set; }
        public double CumulatedAmount { get; set; }
        public float Return { get; set; }
        public float Volatility { get; set; }
        public int NumberOfTransactions { get; set; }

        public string ToCsvHeader(string s = ";")
        {
            var en = ExchangeName?.ToUpper() ?? "COINBASE";
            return $"{en}_PriceOpen{s}{en}_PriceHigh{s}{en}_PriceLow{s}{en}_PriceClose{s}{en}_CumulatedAmount{s}{en}_Return{s}{en}_Volatility{s}{en}_NumberOfTransactions";
        }

        public string ToCsvData(string s = ";")
        {
            return $"{PriceOpen}{s}{PriceHigh}{s}{PriceLow}{s}{PriceClose}{s}{CumulatedAmount}{s}{Return}{s}{Volatility}{s}{NumberOfTransactions}";
        }
    }

    /// <summary>
    /// Currently we support only 1 blockchain
    /// </summary>
    public class AggregatedTransaction
    {
        public DateTime StartTime { get; set; }

        public DateTime EndTime { get; set; }

        public double CumulatedAmountBlockchain { get; set; }

        public double CumulatedAmountBlockchainExCoinbase { get; set; }

        public double AverageFee { get; set; }

        public double AverageInputCount { get; set; }

        public double AverageInputValue { get; set; }

        public double AverageOutputCount { get; set; }

        public double AverageOutputValue { get; set; }

        public double AverageSizeInBytes { get; set; }

        public int NumberOfBlocks { get; set; }

        public int NumberOfTransactions { get; set; }

        public float TransactionsPerBlock => (float)NumberOfTransactions / NumberOfBlocks;

        public List<AggregatedExchangeTransaction> AggregatedExchangeTransactions { get; set; }

        public double SumCumulatedExchangeAmount { get; set; }

        // should be a list, currently only supports two exchanges (items in AggregatedExchangeTransaction list)
        public double ClosePriceDifferenceExchange { get; set; }

        /// <summary>
        /// Writes the data of this object to a csv string
        /// </summary>
        /// <param name="s"></param>
        /// <returns></returns>
        public string ToCsvHeader(string s = ";")
        {
            var csv = $"StartTime{s}EndTime{s}CumulatedAmountBlockchain{s}CumulatedAmountBlockchainExCoinbase{s}AverageFee{s}AverageInputCount{s}AverageInputValue{s}AverageOutputCount{s}AverageOutputValue{s}AverageSizeInBytes{s}NumberOfBlocks{s}NumberOfTransactions{s}TransactionsPerBlock{s}";

            if (AggregatedExchangeTransactions == null || !AggregatedExchangeTransactions.Any())
                AggregatedExchangeTransactions = new List<AggregatedExchangeTransaction> { new AggregatedExchangeTransaction() };

            foreach (var aggregatedExchangeTransaction in AggregatedExchangeTransactions)
            {
                csv += aggregatedExchangeTransaction.ToCsvHeader(s) + s;
            }

            return csv;
        }

        /// <summary>
        /// Writes the data of this object to a csv string
        /// </summary>
        /// <param name="s"></param>
        /// <returns></returns>
        public string ToCsvData(string s = ";")
        {
            var csv = $"{StartTime.ToString()}{s}{EndTime.ToString()}{s}{CumulatedAmountBlockchain}{s}{CumulatedAmountBlockchainExCoinbase}{s}{AverageFee}{s}{AverageInputCount}{s}{AverageInputValue}{s}{AverageOutputCount}{s}{AverageOutputValue}{s}{AverageSizeInBytes}{s}{NumberOfBlocks}{s}{NumberOfTransactions}{s}{TransactionsPerBlock}{s}";

            if (AggregatedExchangeTransactions == null || !AggregatedExchangeTransactions.Any())
                AggregatedExchangeTransactions = new List<AggregatedExchangeTransaction> { new AggregatedExchangeTransaction() };

            foreach (var aggregatedExchangeTransaction in AggregatedExchangeTransactions)
            {
                csv += aggregatedExchangeTransaction.ToCsvData(s) + s;
            }

            return csv;
        }

        public override string ToString()
        {
            return $"{StartTime.ToString()} - {EndTime.ToString()}";
        }
    }

    class Program
    {
        public static List<string> GetFileNamesWithPattern(string directoryPath, string pattern)
        {
            var fileNames = new List<string>();

            foreach (var fileName in Directory.GetFiles(directoryPath).OrderBy(f => f).ToList())
            {
                if (fileName.ToLower().Contains(pattern.ToLower())) fileNames.Add(fileName);
            }

            return fileNames;
        }

        private static readonly int MaxNumOfThreads = 50;

        static void Main(string[] args)
        {
            Console.WriteLine("Let's hack!");
            Console.WriteLine();
            Console.WriteLine("Inputs:");

            var outputFileBlocks = @"C:\Users\weichslu\Documents\Privat\MA\" + $"{DateTime.Now:yyyyMMddHHmm}-AllBlocks.csv";
            var outputFile = @"C:\Users\weichslu\Documents\Privat\MA\" + $"{DateTime.Now:yyyyMMddHHmm}-AggregatedTransactions.csv";
            var dataSrcDir = @"C:\Users\weichslu\Documents\Privat\MA\src";

            var rawBlockFiles = GetFileNamesWithPattern(dataSrcDir, "block");
            var rawBlockchainTransactionFiles = GetFileNamesWithPattern(dataSrcDir, "txes");
            var rawExchangeFiles = GetFileNamesWithPattern(dataSrcDir, "exchange");

            var coinbaseTransactions = GetExchangeTransactionsRaw("Coinbase", rawExchangeFiles.FirstOrDefault(), ',');
            LogExchangeTransaction(coinbaseTransactions);

            var blockchainBlocks = GetBlockchainBlocksRaw("Bitcoin", rawBlockFiles, ';');
            LogBlockchainBlocks(blockchainBlocks);


            Console.WriteLine("----------------");

            var orderedBlocks = blockchainBlocks.AsParallel().WithDegreeOfParallelism(8).OrderBy(b => b.Height);
            //CalcBlockMiningTime(orderedBlocks);
            File.WriteAllLines(outputFileBlocks, orderedBlocks.Select(b => b.ToCsvData()).Prepend(orderedBlocks.FirstOrDefault().ToCsvHeader()).ToArray());
            Console.WriteLine("Block file written: " + outputFileBlocks);

            var intervalMin = 30; // specify interval for aggregation in minutes
            var intervalMs = intervalMin * 60 * 1000;
            var isFirstRun = true;

            foreach (var rawBlockchainTransactionFile in rawBlockchainTransactionFiles)
            {
                var blockchainTransactions = GetBlockchainTransactionsRaw("Bitcoin", new List<string> { rawBlockchainTransactionFile }, ';');
                LogBlockchainTransaction(blockchainTransactions);

                var searchIntervalStartDateTime = blockchainTransactions.FirstOrDefault().Timestamp;
                var searchEndDateTime = blockchainTransactions.LastOrDefault().Timestamp;

                Console.WriteLine("Search start time: " + searchIntervalStartDateTime);
                Console.WriteLine("Search end time: " + searchEndDateTime);

                var transactionQueue =
                    InitAggregatedTransactions(searchIntervalStartDateTime, searchEndDateTime, intervalMs);

                Console.WriteLine("Total #Intervals: " + transactionQueue.Count);

                var aggregatedTransactions =
                    GetAggregatedTransactions(transactionQueue, coinbaseTransactions, blockchainTransactions);

                var orderedTransactions = aggregatedTransactions.AsParallel().WithDegreeOfParallelism(8).OrderBy(t => t.StartTime);
                //CalcExchangeReturn(orderedTransactions);

                if (isFirstRun)
                    File.AppendAllLines(outputFile, orderedTransactions.Select(t => t.ToCsvData()).Prepend(orderedTransactions.FirstOrDefault().ToCsvHeader()).ToArray());
                else
                    File.AppendAllLines(outputFile, orderedTransactions.Select(t => t.ToCsvData()).ToArray());

                isFirstRun = false;
            }


            Console.WriteLine();
            Console.WriteLine($"File {outputFile} written");
            Console.WriteLine("Press any key to end...");

            Console.Read();
        }

        public static void CalcBlockMiningTime(IEnumerable<BlockchainBlock> blocks)
        {
            for (var i = 1; i < blocks.Count(); i++)
            {
                var previousBlock = blocks.ElementAt(i - 1);
                var currentBlock = blocks.ElementAt(i);

                currentBlock.MiningTimeMinutes =
                    (double)(currentBlock.Timestamp.Ticks - previousBlock.Timestamp.Ticks) / 10000000 / 60;
            }
        }

        public static void CalcExchangeReturn(IEnumerable<AggregatedTransaction> aggregatedTransactions)
        {
            for (var i = 1; i < aggregatedTransactions.Count(); i++)
            {
                var previousAggregatedExchangeTransaction =
                    GetPreviousAggregatedExchangeTransaction(aggregatedTransactions, i);
                var aggregatedExchangeTransaction = aggregatedTransactions.ElementAt(i).AggregatedExchangeTransactions.FirstOrDefault();

                if (previousAggregatedExchangeTransaction == null || aggregatedExchangeTransaction == null ||
                    aggregatedExchangeTransaction.PriceClose <= 0 || previousAggregatedExchangeTransaction.PriceClose <= 0) continue;

                aggregatedExchangeTransaction.Return = (float)(aggregatedExchangeTransaction.PriceClose / previousAggregatedExchangeTransaction.PriceClose - 1);
            }
        }

        public static AggregatedExchangeTransaction GetPreviousAggregatedExchangeTransaction(IEnumerable<AggregatedTransaction> aggregatedTransactions, int currentPosition)
        {
            for (var i = currentPosition - 1; i > 0; i--)
            {
                var previousAggregatedExchangeTransaction = aggregatedTransactions.ElementAt(i).AggregatedExchangeTransactions.FirstOrDefault();
                if (previousAggregatedExchangeTransaction == null ||
                    previousAggregatedExchangeTransaction.PriceClose <= 0) continue;
                return previousAggregatedExchangeTransaction;
            }

            return null;
        }

        public static ConcurrentQueue<Tuple<long, long>> InitAggregatedTransactions(DateTime searchStartDateTime, DateTime searchEndDateTime, int intervalMs)
        {
            var intervalQueue = new ConcurrentQueue<Tuple<long, long>>();

            // 10,000 ticks in a millisecond
            var intervalStartTimeTicks = searchStartDateTime.Ticks;
            var endTimeTicks = searchEndDateTime.Ticks;
            var intervalTicks = (long)intervalMs * 10000;

            while (intervalStartTimeTicks < endTimeTicks)
            {
                var intervalEndTimeTicks = intervalStartTimeTicks + intervalTicks;
                intervalQueue.Enqueue(Tuple.Create(intervalStartTimeTicks, intervalEndTimeTicks));
                intervalStartTimeTicks = intervalEndTimeTicks;
            }

            return intervalQueue;
        }

        public static ConcurrentBag<AggregatedTransaction> GetAggregatedBlocks(
            ConcurrentQueue<Tuple<long, long>> queue, List<ExchangeTransaction> coinbaseTransactions, List<BlockchainTransaction> blockchainTransactions)
        {
            var tasks = new List<Task>();
            var bag = new ConcurrentBag<AggregatedTransaction>();

            while (!queue.IsEmpty)
            {
                while (tasks.Any() && tasks.Count(t => t != null && !t.IsCompleted) > MaxNumOfThreads)
                {
                    Thread.Sleep(200);
                }

                tasks.Add(Task.Factory.StartNew(() =>
                {
                    if (queue.TryDequeue(out var intervalTuple))
                    {
                        var aggregatedTransaction = new AggregatedTransaction();
                        aggregatedTransaction.StartTime = new DateTime(intervalTuple.Item1);
                        aggregatedTransaction.EndTime = new DateTime(intervalTuple.Item2);

                        var intervalTransactionsCoinbase = GetIntervalExchangeTransactions(coinbaseTransactions, aggregatedTransaction.StartTime, aggregatedTransaction.EndTime);
                        var intervalTransactionsBlockchain = GetIntervalExchangeTransactions(blockchainTransactions, aggregatedTransaction.StartTime, aggregatedTransaction.EndTime);

                        aggregatedTransaction.CumulatedAmountBlockchain = intervalTransactionsBlockchain.Any() ? ToBitcoin(intervalTransactionsBlockchain.Select(t => t.OutputAdjusted)?.Sum() ?? -1) : -1;
                        aggregatedTransaction.CumulatedAmountBlockchainExCoinbase = ToBitcoin(intervalTransactionsBlockchain.Any() ? intervalTransactionsBlockchain.Where(t => !t.IsCoinbase).Select(t => t.OutputAdjusted)?.Sum() ?? -1 : -1);
                        aggregatedTransaction.AverageFee = ToBitcoin(intervalTransactionsBlockchain.Any() ? intervalTransactionsBlockchain.Select(t => t.Fee)?.Average() ?? -1 : -1);
                        aggregatedTransaction.AverageInputCount = intervalTransactionsBlockchain.Any() ? intervalTransactionsBlockchain.Select(t => t.InputCount)?.Average() ?? -1 : -1;
                        aggregatedTransaction.AverageInputValue = ToBitcoin(intervalTransactionsBlockchain.Any() ? intervalTransactionsBlockchain.Select(t => t.InputValue)?.Average() ?? -1 : -1);
                        aggregatedTransaction.AverageOutputCount = intervalTransactionsBlockchain.Any() ? intervalTransactionsBlockchain.Select(t => t.OutputCount)?.Average() ?? -1 : -1;
                        aggregatedTransaction.AverageOutputValue = ToBitcoin(intervalTransactionsBlockchain.Any() ? intervalTransactionsBlockchain.Select(t => t.OutputValue)?.Average() ?? -1 : -1);
                        aggregatedTransaction.AverageSizeInBytes = intervalTransactionsBlockchain.Any() ? intervalTransactionsBlockchain.Select(t => t.SizeBytes)?.Average() ?? -1 : -1;
                        aggregatedTransaction.NumberOfBlocks = intervalTransactionsBlockchain.Any() ? intervalTransactionsBlockchain.Select(t => t.BlockHeight)?.Distinct()?.Count() ?? -1 : -1;
                        aggregatedTransaction.NumberOfTransactions = intervalTransactionsBlockchain.Any() ? intervalTransactionsBlockchain.Count() : -1;
                        aggregatedTransaction.AggregatedExchangeTransactions = new List<AggregatedExchangeTransaction>();

                        if (intervalTransactionsCoinbase.Any())
                        {
                            var currentAggregatedTransactionCoinbase = GetAggregatedExchangeTransaction(intervalTransactionsCoinbase, null);
                            aggregatedTransaction.AggregatedExchangeTransactions.Add(currentAggregatedTransactionCoinbase);
                        }

                        aggregatedTransaction.SumCumulatedExchangeAmount = aggregatedTransaction.AggregatedExchangeTransactions.Select(l => l.CumulatedAmount)?.Sum() ?? -1;

                        if (aggregatedTransaction.AggregatedExchangeTransactions.Count == 2) aggregatedTransaction.ClosePriceDifferenceExchange = aggregatedTransaction.AggregatedExchangeTransactions[0].PriceClose / aggregatedTransaction.AggregatedExchangeTransactions[1].PriceClose;

                        Console.WriteLine(aggregatedTransaction.ToCsvData());
                        bag.Add(aggregatedTransaction);
                    }
                }));
            }

            try
            {
                Task.WaitAll(tasks.ToArray());
            }
            catch (Exception)
            {
                while (tasks.Any() && tasks.Count(t => t != null && !t.IsCompleted) > 0)
                {
                    Thread.Sleep(200);
                }
            }

            return bag;
        }

        public static ConcurrentBag<AggregatedTransaction> GetAggregatedTransactions(
            ConcurrentQueue<Tuple<long, long>> queue, List<ExchangeTransaction> coinbaseTransactions, List<BlockchainTransaction> blockchainTransactions)
        {
            var tasks = new List<Task>();
            var bag = new ConcurrentBag<AggregatedTransaction>();

            while (!queue.IsEmpty)
            {
                while (tasks.Any() && tasks.Count(t => t != null && !t.IsCompleted) > MaxNumOfThreads)
                {
                    Thread.Sleep(200);
                }

                tasks.Add(Task.Factory.StartNew(() =>
                {
                    if (queue.TryDequeue(out var intervalTuple))
                    {
                        var aggregatedTransaction = new AggregatedTransaction();
                        aggregatedTransaction.StartTime = new DateTime(intervalTuple.Item1);
                        aggregatedTransaction.EndTime = new DateTime(intervalTuple.Item2);

                        var intervalTransactionsCoinbase = GetIntervalExchangeTransactions(coinbaseTransactions, aggregatedTransaction.StartTime, aggregatedTransaction.EndTime);
                        var intervalTransactionsBlockchain = GetIntervalExchangeTransactions(blockchainTransactions, aggregatedTransaction.StartTime, aggregatedTransaction.EndTime);

                        aggregatedTransaction.CumulatedAmountBlockchain = intervalTransactionsBlockchain.Any() ? ToBitcoin(intervalTransactionsBlockchain.Select(t => t.OutputAdjusted)?.Sum() ?? -1) : -1;
                        aggregatedTransaction.CumulatedAmountBlockchainExCoinbase = ToBitcoin(intervalTransactionsBlockchain.Any() ? intervalTransactionsBlockchain.Where(t => !t.IsCoinbase).Select(t => t.OutputAdjusted)?.Sum() ?? -1 : -1);
                        aggregatedTransaction.AverageFee = ToBitcoin(intervalTransactionsBlockchain.Any() ? intervalTransactionsBlockchain.Select(t => t.Fee)?.Average() ?? -1 : -1);
                        aggregatedTransaction.AverageInputCount = intervalTransactionsBlockchain.Any() ? intervalTransactionsBlockchain.Select(t => t.InputCount)?.Average() ?? -1 : -1;
                        aggregatedTransaction.AverageInputValue = ToBitcoin(intervalTransactionsBlockchain.Any() ? intervalTransactionsBlockchain.Select(t => t.InputValue)?.Average() ?? -1 : -1);
                        aggregatedTransaction.AverageOutputCount = intervalTransactionsBlockchain.Any() ? intervalTransactionsBlockchain.Select(t => t.OutputCount)?.Average() ?? -1 : -1;
                        aggregatedTransaction.AverageOutputValue = ToBitcoin(intervalTransactionsBlockchain.Any() ? intervalTransactionsBlockchain.Select(t => t.OutputValue)?.Average() ?? -1 : -1);
                        aggregatedTransaction.AverageSizeInBytes = intervalTransactionsBlockchain.Any() ? intervalTransactionsBlockchain.Select(t => t.SizeBytes)?.Average() ?? -1 : -1;
                        aggregatedTransaction.NumberOfBlocks = intervalTransactionsBlockchain.Any() ? intervalTransactionsBlockchain.Select(t => t.BlockHeight)?.Distinct()?.Count() ?? -1 : -1;
                        aggregatedTransaction.NumberOfTransactions = intervalTransactionsBlockchain.Any() ? intervalTransactionsBlockchain.Count() : -1;
                        aggregatedTransaction.AggregatedExchangeTransactions = new List<AggregatedExchangeTransaction>();

                        if (intervalTransactionsCoinbase.Any())
                        {
                            var currentAggregatedTransactionCoinbase = GetAggregatedExchangeTransaction(intervalTransactionsCoinbase, null);
                            aggregatedTransaction.AggregatedExchangeTransactions.Add(currentAggregatedTransactionCoinbase);
                        }

                        aggregatedTransaction.SumCumulatedExchangeAmount = aggregatedTransaction.AggregatedExchangeTransactions.Select(l => l.CumulatedAmount)?.Sum() ?? -1;

                        if (aggregatedTransaction.AggregatedExchangeTransactions.Count == 2) aggregatedTransaction.ClosePriceDifferenceExchange = aggregatedTransaction.AggregatedExchangeTransactions[0].PriceClose / aggregatedTransaction.AggregatedExchangeTransactions[1].PriceClose;

                        Console.WriteLine(aggregatedTransaction.ToCsvData());
                        bag.Add(aggregatedTransaction);
                    }
                }));
            }

            try
            {
                Task.WaitAll(tasks.ToArray());
            }
            catch (Exception)
            {
                while (tasks.Any() && tasks.Count(t => t != null && !t.IsCompleted) > 0)
                {
                    Thread.Sleep(200);
                }
            }

            return bag;
        }

        public static AggregatedExchangeTransaction GetAggregatedExchangeTransaction(List<ExchangeTransaction> transactions, AggregatedExchangeTransaction lastAggregatedExchangeTransaction)
        {
            var aggregatedExchangeTransaction = new AggregatedExchangeTransaction();

            var firstTransaction = transactions.FirstOrDefault();
            var transactionPrices = transactions.Select(t => t.Price);

            aggregatedExchangeTransaction.ExchangeName = firstTransaction.Name;
            aggregatedExchangeTransaction.PriceOpen = firstTransaction.Price;
            aggregatedExchangeTransaction.PriceHigh = transactionPrices.Max();
            aggregatedExchangeTransaction.PriceLow = transactionPrices.Min();
            aggregatedExchangeTransaction.PriceClose = transactions.LastOrDefault().Price;
            aggregatedExchangeTransaction.CumulatedAmount = transactions.Select(t => t.Amount).Sum();
            aggregatedExchangeTransaction.NumberOfTransactions = transactions.Count();

            if (lastAggregatedExchangeTransaction == null) return aggregatedExchangeTransaction;

            aggregatedExchangeTransaction.Return = (float)(aggregatedExchangeTransaction.PriceClose / lastAggregatedExchangeTransaction.PriceClose - 1);

            return aggregatedExchangeTransaction;
        }

        public static List<BlockchainTransaction> GetIntervalExchangeTransactions(List<BlockchainTransaction> transactions, DateTime startTime, DateTime endTime)
        {
            return transactions.Where(t => t.Timestamp >= startTime && t.Timestamp < endTime)?.ToList() ?? new List<BlockchainTransaction>();
        }

        public static List<ExchangeTransaction> GetIntervalExchangeTransactions(List<ExchangeTransaction> transactions, DateTime startTime, DateTime endTime)
        {
            return transactions.Where(t => t.Timestamp >= startTime && t.Timestamp < endTime)?.ToList() ?? new List<ExchangeTransaction>();
        }

        public static void LogExchangeTransaction(List<ExchangeTransaction> transactions)
        {
            Console.WriteLine("---");
            Console.WriteLine($"{transactions.Count} {transactions.FirstOrDefault().Name} transactions found");
            Console.WriteLine($"1st { transactions.FirstOrDefault()}");
            Console.WriteLine($"Last { transactions.LastOrDefault()}");
        }

        public static void LogBlockchainTransaction(List<BlockchainTransaction> transactions)
        {
            Console.WriteLine("---");
            Console.WriteLine($"{transactions.Count} {transactions.FirstOrDefault().Name} transactions found");
            Console.WriteLine($"1st { transactions.FirstOrDefault()}");
            Console.WriteLine($"Last { transactions.LastOrDefault()}");
        }

        public static void LogBlockchainBlocks(List<BlockchainBlock> blocks)
        {
            Console.WriteLine("---");
            Console.WriteLine($"{blocks.Count} {blocks.FirstOrDefault().Name} blocks found");
            Console.WriteLine($"1st { blocks.FirstOrDefault()}");
            Console.WriteLine($"Last { blocks.LastOrDefault()}");
        }

        public static DateTime UnixTimeStampToDateTime(double unixTimeStamp)
        {
            // Unix timestamp is seconds past epoch
            var dtDateTime = new DateTime(1970, 1, 1, 0, 0, 0, 0, System.DateTimeKind.Utc);
            dtDateTime = dtDateTime.AddSeconds(unixTimeStamp).ToLocalTime();
            return dtDateTime;
        }

        public static List<ExchangeTransaction> GetExchangeTransactionsRaw(string exchangeName, string filename, char splitSign)
        {
            var transactions = new List<ExchangeTransaction>();

            using (var reader = new StreamReader(new FileStream(filename, FileMode.Open)))
            {
                string line;

                while ((line = reader.ReadLine()) != null)
                {
                    if (line.ToLower().Contains("nan")) continue;
                    var splitted = line.Split(splitSign);
                    if (splitted.Length != 8) continue;

                    transactions.Add(new ExchangeTransaction
                    {
                        Name = exchangeName,
                        Timestamp = UnixTimeStampToDateTime(Double.Parse(splitted[0].Replace(".", ","), NumberStyles.Float)),
                        Price = double.Parse(splitted[1].Replace(".", ","), NumberStyles.Float),
                        Amount = double.Parse(splitted[2].Replace(".", ","), NumberStyles.Float)
                    });
                }
            }

            return transactions.OrderBy(t => t.Timestamp).ToList();
        }

        public static List<BlockchainTransaction> GetBlockchainTransactionsRaw(string blockchainName, List<string> filenames, char splitSign)
        {
            var transactions = new List<BlockchainTransaction>();

            foreach (var filename in filenames)
            {
                using (var reader = new StreamReader(new FileStream(filename, FileMode.Open)))
                {
                    string line;
                    var skipLine = true;

                    while ((line = reader.ReadLine()) != null)
                    {
                        if (skipLine)
                        {
                            skipLine = false;
                            continue;
                        }

                        var splitted = line.Split(splitSign);
                        if (splitted.Length != 11) continue;

                        transactions.Add(new BlockchainTransaction
                        {
                            Name = blockchainName,
                            Timestamp = UnixTimeStampToDateTime(Double.Parse(splitted[0].Replace(".", ","), NumberStyles.Float)),
                            BlockHeight = int.Parse(splitted[1].Replace(".", ","), NumberStyles.Integer),
                            SizeBytes = int.Parse(splitted[2].Replace(".", ","), NumberStyles.Integer),
                            IsCoinbase = bool.Parse(splitted[3].Replace(".", ",")),
                            Locktime = double.Parse(splitted[4].Replace(".", ","), NumberStyles.Integer),
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

            return transactions.OrderBy(t => t.Timestamp).ToList();
        }

        public static List<BlockchainBlock> GetBlockchainBlocksRaw(string blockchainName, List<string> filenames, char splitSign)
        {
            var blocks = new List<BlockchainBlock>();

            foreach (var filename in filenames)
            {
                using (var reader = new StreamReader(new FileStream(filename, FileMode.Open)))
                {
                    string line;
                    var skipLine = true;

                    while ((line = reader.ReadLine()) != null)
                    {
                        if (skipLine)
                        {
                            skipLine = false;
                            continue;
                        }

                        var splitted = line.Split(splitSign);
                        if (splitted.Length != 13) continue;

                        blocks.Add(new BlockchainBlock
                        {
                            Name = blockchainName,
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

            return blocks.OrderBy(t => t.Timestamp).ToList();
        }

        public static double ToBitcoin(double satoshi)
        {
            if (satoshi == -1) return satoshi;

            return satoshi / 100000000;
        }
    }
}
