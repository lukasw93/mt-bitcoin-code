# Aggregation Code thesis
## A quantitative analysis of the Bitcoin blockchain

This repository contains code that was used to aggregate Bitcoin related data from different data sources.

### Data Source: Blockchain

BlockSci was used to extract the necessary data directly from the Bitcoin blockchain.
Huge CSV files can be created by Python queries. But as the blockchain has no price information of Bitcoin to any given time another data source has to be used.

### Data Source: Crypto Exchange

Historical price information can be found on the web on a lot of different platforms.
But most of the times historical prices are delivered on a daily basis.
If you need to have more accurate data, it can get a bit more complicated as just downloading a spreadsheet.
Luckily kaggle.com offers historical price information on Bitcoin on a minute basis here: https://www.kaggle.com/mczielinski/bitcoin-historical-data. 
But still, this information must be aggregated with the Blockchain data.

## Projects in this repo

#### BlockchainDataAggregator (deprecated)
Contains code to aggregate blockchain and exchange transactions. This code is deprecated and just stored for backup reasons.

#### BlockchainExchangeAggregator
This code was used for the master thesis. It reads the input files for block and transaction blockchain data as well as for exchange data and aggregates the data with multithreading to a single aggregated data set. The current defined aggregation interval is 30 minutes. So theoretically all transactions within 3 blocks are enriched with the price information (OHLC) of a crypto exchange and aggregated into 30 minutes data sets for subsequent analysis.

#### ExchangeDateAggregator
With this code aggregate the data output from the project "MergeTimestampWithDataFile" on a customizable basis (seconds, minutes or hours).

#### MergeTimestampWithDataFile
In case you have two separate files for timestamps and amounts/prices, with this code both files can be integrated into one single file.
