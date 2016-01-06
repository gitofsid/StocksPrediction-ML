'''
sbaronia - this file has few methods defined that are avaialble in Spark MLlib
We save model, generate training and testing graphs, calculate rms error and 
trend error
'''

from pyspark import SparkConf, SparkContext
import sys, string
from pyspark.mllib.classification import NaiveBayes, NaiveBayesModel, LogisticRegressionWithLBFGS, LogisticRegressionModel
from pyspark.mllib.regression import LinearRegressionWithSGD, LinearRegressionModel
from pyspark.mllib.regression import LabeledPoint
import numpy as np
import math

# plotting related 
import matplotlib.pyplot as plt
import matplotlib.mlab as mlab
import matplotlib.cbook as cbook
import matplotlib.ticker as ticker


'''
=======================================
   	         ALL METHODS
Few Methods Are Not Available in MLlib
=======================================
'''

''' Naive Bayes 
   ============= '''
# output_n 
# 0 - Next Day Actual Opening 	- 6 actual mapping col 1
# 1 - Next Day Actual  High 	-7  col2
# 2 - Next Day Actual Low	-8 col3
# 3 - Next Day Actual Close	 -9 col 4
# 4 - Next Day Actual Volume -10 col5
def naiveBayes(features,sc,output_n):
	''' calling NaiveBayes with and training using our data set '''
	features_and_label = features.collect()
	training_features_labels = features_and_label[0:70]
	
	testing_features_labels = features_and_label[70:116]
	


	labeled_training = []
	for x in training_features_labels:
		labeled_training.append(LabeledPoint(x[0],x[1]))

	naivebayes_model = NaiveBayes.train(sc.parallelize(labeled_training),1.0)


	predictions = []
	
	for efeature in testing_features_labels:

		testing_data = LabeledPoint(efeature[0],efeature[1])
		
		prediction = naivebayes_model.predict(testing_data.features)

		predictions.append([testing_data.label,float(prediction)])

		labeled_training.append(testing_data)

		naivebayes_model = NaiveBayes.train(sc.parallelize(labeled_training),1.0)
			
	return naivebayes_model,predictions

''' Linear Regression
    =================='''
def linearRegression(features,sc,output_n):
	features_and_label = features.collect()
	training_features_labels = features_and_label[0:70]
	
	testing_features_labels = features_and_label[70:116]
	
	
	linearregression_model = LinearRegressionWithSGD.train(training_data,iterations=0,regParam=200)
	prediction = testing_data.map(lambda line: (line.label, linearregression_model.predict(line.features)))
	return linearregression_model,prediction

# # not in pyspark
# def logisticRegression(features,sc,output_n):
# 	training_data, testing_data = features_and_label.randomSplit([0.8, 0.2], seed = 0)
# 	logregression_model = LogisticRegressionWithLBFGS.train(training_data)
# 	prediction = testing_data.map(lambda line: (line.label, logregression_model.predict(line.features)))
# 	return prediction 

# # not in pyspark
# def multilayerPerceptron(features,sc,output_n):
# 	return

'''
====================
   METHOD TO CALL
====================
'''
def call_the_method(input_number,features_labels,sc,output_n):
	if input_number == 1:
		print "Calling Naive Bayes"
		model,prediciton = naiveBayes(features_labels,sc,output_n)
	elif input_number == 2:
		model,prediciton = logisticRegression(features_labels,sc,output_n)
	elif input_number == 3:
		print "Calling Linear Regression"
		model,prediciton = linearRegression(features_labels,sc,output_n)
	elif input_number == 4:
		model,prediciton = multilayerPerceptron(features_labels,sc,output_n)
	else:
		print "Wrong method selected"
		sys.exit(1)

	return model,sc.parallelize(prediciton)

'''
==============
   GRAPHS
==============
'''

def ylabel_graph(output_n):
	ylabel = int(output_n) - 1
	if ylabel == 0:
		return 'Next Day Opening'
	elif ylabel == 1:
		return 'Next Day High'
	elif ylabel == 2:
		return 'Next Day Low'
	elif ylabel == 3:
		return 'Next Day Close'
	elif ylabel == 4:
		return 'Next Day Volume'
	else:
		return 'Label Missing'

def draw_graph(prediction,output_needed,error,count,company):
	''' graphs'''
	predicted_value_list = prediction.map(lambda (x,y): y).collect()
	actual_value_list = prediction.map(lambda (x,y): x).collect()
	fig, ax = plt.subplots()
	ax.plot(range(len(actual_value_list)),actual_value_list, 'o-', color='b', label='Actual',linewidth=3)
	ax.plot(range(len(predicted_value_list)),predicted_value_list, 'o-', color='r', label='Prediction',linewidth=3)
	plt.xlabel('Days',fontweight='bold')
	plt.ylabel('Next Day Close',fontweight='bold')
	plt.legend(loc='best')
	title = "Stock: " +  company + " | " + "Error: " + str(count)  + " | " + "RMS: " + str(error)
	plt.title(title,fontweight='bold')
	plt.show()
	return


'''
==================
  FILE PARSING
==================
'''

def parseNeededFeatureAndLabel(line,features_n,output_n):
	features_set = []

	elements = line.strip().split(',')
	ind_feature = features_n.split(',')

	for feat in ind_feature:
		if feat is   'A':
			features_set.append(elements[6])
		elif feat is 'B':
			features_set.append(elements[7])
		elif feat is 'C':
			features_set.append(elements[8])
		elif feat is 'D':
			features_set.append(elements[9])
		elif feat is 'E':
			features_set.append(elements[10])
		elif feat is 'F':
			features_set.append(elements[11])
		elif feat is 'G':
			features_set.append(elements[12])
		elif feat is 'H':
			features_set.append(elements[13])
		elif feat is 'I':
			features_set.append(elements[14])
		elif feat is 'J':
			features_set.append(elements[15])
		elif feat is 'K':
			features_set.append(elements[16])
		elif feat is 'L':
			features_set.append(elements[17])
		elif feat is 'M':
			features_set.append(elements[18])
		elif feat is 'N':
			''' extending when we are using sublist instead of one 
			element as this makes finallist defective'''
			features_set.extend(elements[19:])
		elif feat is 'O':
			features_set.extend(elements[6:19])
		elif feat is 'P':
			features_set.extend(elements[6:])
		else:
			print "Getting wrong feature request"
			sys.exit(1)
	

	return [float(elements[int(output_n)-1]),features_set]


def main():
	method_needed = sys.argv[1]
	stock_file = sys.argv[2]
	company = sys.argv[3]
	output_model = sys.argv[4]
	features_needed = sys.argv[5]
	output_needed = sys.argv[6]


	conf = SparkConf().setAppName('Stock Prediction Using Machine Learning')
	sc = SparkContext(conf=conf)
	assert sc.version >= '1.5.1'

	''' extracting the header of CSV file'''
	file_data_all = sc.textFile(stock_file)
	file_header = file_data_all.first()
	file_data = file_data_all.filter(lambda line: line != file_header).cache()

	features_labels = file_data.map(lambda line: parseNeededFeatureAndLabel(line,features_needed,output_needed)).cache()
	
	model,prediction = call_the_method(int(method_needed),features_labels,sc,(int(output_needed)-1))

	print "\n**********************\n"
	print " Predictions := \n" 
	print "**********************\n"

	print(prediction.collect())

	act_pred = prediction.collect()

	ap_0 = act_pred[0]
	count = 0
	for ap in act_pred[1:]:
		if (ap[0] > ap_0[0] and ap[1] < ap_0[1]) or (ap[0] < ap_0[0] and ap[1] > ap_0[1]):
			count = count + 1
		ap_0 = ap

	print "\n**********************************************\n"
	print " Number of directional mismatch := %d\n" % count 
	print "***********************************************\n"

	
						   
	error_num = prediction.map(lambda (x, y): (x - y)**2).reduce(lambda x, y: x + y)
	error = float(math.sqrt(error_num/prediction.count()))

	print "\n**********************\n"
	print " Error := %f \n"  % error  
	print "**********************\n"

	print "\n**********************\n"
	print " Graphs := \n" 
	print "**********************\n"

	draw_graph(prediction,output_needed,error,count,company)

	print "\n**********************\n"
	print " Saving Model := \n" 
	print "**********************\n"

	model.save(sc, output_model)
			
	
if __name__ == "__main__":
	if (len(sys.argv) != 7):
		print "Usage: Please see usage in call_classifiers_for_stocks.py"
		sys.exit(1)
	main()