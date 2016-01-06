'''
sbaronia - SVM classifier
'''
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import split
import sys, string
from pyspark.mllib.classification import SVMWithSGD, SVMModel
from pyspark.mllib.regression import LabeledPoint

''' Parsing the stock file and retruning LabeledPoint RDD
that has target to predict and features. Here we are predicting
5 different values using 335 features'''

def parseNextDayActualOpening(line):
	elements = line.strip().split(',')
	return LabeledPoint(elements[0], elements[6:])

def parseNextDayActualHigh(line):
	elements = line.strip().split(',')
	return LabeledPoint(float(elements[1]), float(elements[6:]))

def parseNextDayActualLow(line):
	elements = line.strip().split(',')
	return LabeledPoint(float(elements[2]), float(elements[6:]))

def parseNextDayActualClose(line):
	elements = line.strip().split(',')
	return LabeledPoint(float(elements[3]), float(elements[6:]))

def parseNextDayActualVolume(line):
	elements = line.strip().split(',')
	return LabeledPoint(float(elements[4]), float(elements[6:]))


def main():
	stock_file = sys.argv[1]
	output_predict_file = sys.argv[2]

	conf = SparkConf().setAppName('Stock Prediction Machine Learning with Twitter')
	sc = SparkContext(conf=conf)
	assert sc.version >= '1.5.1'

	''' extracting the header of CSV file'''
	file_data_all = sc.textFile(stock_file)
	file_header = file_data_all.first()
	file_data = file_data_all.filter(lambda line: line != file_header).cache()

	''' for five different predictions getting data '''
	parsedFileData_NextDayActualOpening = file_data.map(parseNextDayActualOpening)
	parsedFileData_NextDayActualHigh = file_data.map(parseNextDayActualHigh)
	parsedFileData_NextDayActualLow = file_data.map(parseNextDayActualLow)
	parsedFileData_NextDayActualClose = file_data.map(parseNextDayActualClose)
	parsedFileData_NextDayActualVolume = file_data.map(parseNextDayActualVolume)

	print(parsedFileData_NextDayActualOpening.collect())

	''' calling SVM with Stochastic Gradient Descent and
	training using our data set '''
	svm_model_nxtdayactopn = SVMWithSGD.train(parsedFileData_NextDayActualOpening, iterations=10)

	lpreds = parsedFileData_NextDayActualOpening.map(lambda line: (line.label, svm_model_nxtdayactopn.predict(line.features)))

	print(lpreds.collect())

			
	
if __name__ == "__main__":
	if (len(sys.argv) != 3):
		print "Usage: Please provide a stock file to be read and output file name" + \
		"spark-submit --master local[*] svm_predict_stocks.py inputstock output"
		sys.exit(1)
	main()