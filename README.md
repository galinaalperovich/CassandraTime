<h3>“Cassandra and Time Series analysis”</h3>
Final project on Big Data Analytics course in Harvard Extension school

Student name: Galina Alperovich

May, 2017

<h3>Abstract</h3>

One of the most common use cases for Cassandra NoSQL is tracking time-series data. The reason for this is Cassandra’s data model which is an excellent fit for handling data in sequence regardless of datatype or size. Cassandra has highly fast writing speed, built-in replication across nodes and high availability with no single point of failure. Since NoSQL databases developed in a different way than traditional RDMS, you will not find ad-hoc quires for joining, grouping and other standard operations from SQL. 


The purpose of this project is to demonstrate how Cassandra can be used for financial time series analysis and explain why it is natural for Cassandra to work with sequential data. Also we present lightweight web application where the user can select one of the 3000 US companies and play with the time series representing the stock data. The user will be able to draw the series, discover basic statistics, aggregate daily data in a different way and make forecast in real time. The application is fully developed in Python, with Cassandra NoSQL database for data storing, Bokeh framework for interactive Python visualization and Prophet framework for fast and automatic time series forecasting recently open-sourced by Facebook. 


The web application is hosted on AWS and available online. Full step-by-step documentation from project start to deployment is provided. You can reproduce the same application from scratch.

**Application URL:** http://34.202.35.11:5006/CassandraTime (UPDATE: instance shutted down)

YouTube video (2 min): https://youtu.be/9UKfyR77bG8

YouTube video (15 min): https://youtu.be/4VBh6UQd6z8

Full documentation with the report read in Documentation.pdf

