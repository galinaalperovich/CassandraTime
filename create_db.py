# quandl.ApiConfig.api_key = 's7WsB1vNHrGivWgjQZdh'


"""
How to:

1. Install cassandra

pip install cassandra-driver

pip install cql
brew install cassandra
cp /usr/local/Cellar/cassandra/<VERSION 3.10_1>/homebrew.mxcl.cassandra.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/homebrew.mxcl.cassandra.plist

2. Download data as zip archive from this link: https://www.quandl.com/product/WIKIP/WIKI/PRICES-Quandl-End-Of-Day-Stocks-Info
3. Unzip it and rename to WIKI_PRICE.csv (~1.8GB)

4. Run in terminal: cqlsh
5. Execute Commands #1,#2,#3

Command #1:  CREATE KEYSPACE wiki_price_keyspace WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '2'}
Command #2:  USE wiki_price_keyspace
Command #3:  COPY wikiprice FROM 'WIKI_PRICES.csv' WITH HEADER = true;

"""

from cassandra.cluster import Cluster
from cassandra.cqlengine import connection

KEYSPACE = "wiki_price_keyspace"

connection.setup(['127.0.0.1'])
cluster = Cluster()
session = cluster.connect()

# Command #1 to create keyspace
session.execute("""
        CREATE KEYSPACE %s
        WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '2' }
        """ % KEYSPACE)

session.execute("USE %s" % KEYSPACE)

# Command #2 to create table
session.execute("""   CREATE TABLE wikiprice (
        ticker text,
        date timestamp,
        openp float,
        high float,
        low float,
        close float,
        volume float,
        ex_dividend float,
        split_ratio float,
        adj_open float,
        adj_high float,
        adj_low float,
        adj_close float,
        adj_volume float ,
      PRIMARY KEY (ticker, date))
""")

# Create Ticker table
session.execute("""   CREATE TABLE tickers (
        ticker text,
        name text,
      PRIMARY KEY (ticker))
""")


# COPY tickers FROM 'Tickers.csv' WITH HEADER = true ;

# ssh -i bitnami_cassanda.pem bitnami@ec2-54-174-173-94.compute-1.amazonaws.com
# cqlsh -u cassandra -p Wb2SqZx0b87K
# ALTER USER cassandra with PASSWORD 'cassandra';
# CREATE KEYSPACE wiki_price_keyspace
#    ...         WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '2' };
# Create table1
# Create table2
# COPY tickers FROM 'Tickers.csv' WITH HEADER = true ;
#




# scp -i bitnami_cassanda.pem WIKI_PRICES.csv bitnami@ec2-54-174-173-94.compute-1.amazonaws.com:WIKI_PRICES.csv
# scp -i bitnami_cassanda.pem WIKI_PRICES.csv bitnami@ec2-54-174-173-94.compute-1.amazonaws.com:WIKI_PRICES.csv
