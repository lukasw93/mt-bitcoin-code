using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace MergeTimestampWithDataFile
{
    public class ExchangeTransaction
    {
        public string Timestamp { get; set; }

        public string Price { get; set; }

        public string Amount { get; set; }

        public override string ToString()
        {
            return $"{Timestamp},{Price},{Amount}";
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            var timestampFile = @"C:\Users\weichslu\Documents\Privat\SE aus Finance\Coinbase\UNIX.csv";
            var dataFile = @"C:\Users\weichslu\Documents\Privat\SE aus Finance\Coinbase\BTC.csv";
            var outputFile = @"C:\Users\weichslu\Documents\Privat\SE aus Finance\Coinbase\CoinbaseTransactions.csv";

            var exchangeTransactions = new List<ExchangeTransaction>();

            // read timestamp file
            using (var reader = new StreamReader(new FileStream(timestampFile, FileMode.Open)))
            {
                string line;

                while ((line = reader.ReadLine()) != null)
                {
                    exchangeTransactions.Add(new ExchangeTransaction
                    {
                        Timestamp = line
                    });
                }
            }

            using (var reader = new StreamReader(new FileStream(dataFile, FileMode.Open)))
            {
                string line;
                int i = 0;

                while ((line = reader.ReadLine()) != null)
                {
                    // break if number of transactions exceed number of timestamps
                    if (i > exchangeTransactions.Count)
                        break;

                    var splitted = line.Split(',');
                    if (splitted.Length != 2) continue;

                    exchangeTransactions[i].Price = splitted[0];
                    exchangeTransactions[i].Amount = splitted[1];
                    i++;
                }
            }

            Console.WriteLine(exchangeTransactions.Count + " Exchange Transactions found");

            File.WriteAllLines(outputFile, exchangeTransactions.Select(t => t.ToString()).ToArray());


            Console.Read();
        }
    }
}
