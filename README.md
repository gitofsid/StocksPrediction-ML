Project
========

With an unprecedented growth of social media platforms such as Facebook and Twitter, the amount and type of digital content available has started affecting different aspects of business world and stock price is not an uncharted territory anymore. Here, a trial is made to relate how Twitter world affects stock price of a company given it has a significant presence on social media.

To work on this problem, stocks of five companies which have significant presence on social media are considered, namely – Google, Microsoft, Facebook, Shopify and ZS Pharma. The quantity we are predicting is – Stock’s Next Day Closing Value, Root Mean Squared error and the trend Directional Error. Along with stock related data, Sentiment Analysis on Twitter data corresponding to all the five companies under question is conducted to be able to use tweets as input features.  Three different machine learning algorithms are used– Naïve Bayes, Linear Regression and Recurrent Neural Network, to carry out this task and to compare results from each method through graphical visualization. As an extension to the task, results from each method with and without Twitter sentiments are compared in combination with other relevant information such as NASDAQ Computer and Dow Jones indexes and previous days’ stock values. Apache Spark MLlib, Matlab and Stanford’s CoreNLP libraries are used to implement the algorithms and sentimental analysis. Following is an introduction to machine learning’s three algorithms we used for analysis.

In this repo I have only provided the csv files for Google and Facebook. The method currently used is only Naive Bayes that is well documented in the python code files. Linear regression has also been implemented for testing purposes. The graphs directory contains downloadable raw files that show the trend of stocks with Twitter and without Twitter data for Naive Bayes. 

The stock data under consideration is from August 2015 - November 2015.


Usage
=====

Use following command to start the analysis - 

Google - python call_classifiers_for_stocks.py input_files/goog.csv_merged.csv output Google
Facebook - python call_classifiers_for_stocks.py input_files/fb.csv_merged.csv output Facebook

Output directory "output" will be created that save our model.