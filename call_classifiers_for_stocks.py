'''
sbaronia - this file calls the classifiers_for_stocks file with correct 
method that we want to use 
'''


import sys
import os
import shutil

def ask_for_algorithm():
	print "Enter the method you want to use (one method): \n" + \
	"Naive Bayes' Classification (1) \n" + \
	"Logistic Regression (2) \n" + \
	"Linear Regression (3) \n" + \
	"Multilayer Preceptron Neural Network (4) \n"
	method_needed = raw_input("Enter the method you want to try: ")
	if method_needed in '1,2,3,4':
		return method_needed
	else:
		print "Please give one number in range"
		ask_for_algorithm()

def ask_for_output():
	print "Enter what you want to predict (one value):\n" + \
	"Next Day Actual Opening (1) \n" + \
	"Next Day Actual High (2) \n" + \
	"Next Day Actual Low (3)\n" + \
	"Next Day Actual Closing (4)\n" + \
	"Next Day Actual Volume (5)\n"
	output_needed = raw_input('Enter the output number: ')
	if output_needed in '1,2,3,4,5':
		return output_needed
	else:
		print "Please give one number in range"
		ask_for_output()

def ask_for_features():
	print "Enter features you want to test with:\n" + \
	"Stock Open (A)\n" + \
	"Stock High (B)\n" + \
	"Stock Low (C)\n" + \
	"Stock Close (D)\n" + \
	"Stock Volume (E)\n" + \
	"DOW JONES Open (F)\n" + \
	"DOW JONES High (G)\n" + \
	"DOW JONES Low (H)\n" + \
	"DOW JONES Close (I)\n" + \
	"NASDAQ Computer Open (J)\n" + \
	"NASDAQ Computer High (K)\n" + \
	"NASDAQ Computer Low (L)\n" + \
	"NASDAQ Computer Close (M)\n" + \
	"Sentiments from 335 Twitter Accounts (N)\n" + \
	"Everything Except Twitter (O)\n" + \
 	"All Features (P)\n"
	features_input = raw_input("Enter the features number (comma separated): ")
	if set(features_input).issubset('A,B,C,D,E,F,G,H,I,J,K,L,M,N,O,P'):
		if 'P' in features_input:
			print "Using All Feature (P)"
			return 'P'
		elif 'O' in features_input:
			print "Everything except Twitter (O)"
			return 'O'
		else:
			return features_input
	else:
		print "\nPlease give the character in range"
		ask_for_features()

def main():
	stock_file = sys.argv[1]
	output_model = sys.argv[2]
	company = sys.argv[3]
	method_needed = ask_for_algorithm()
	output_needed = ask_for_output()
	features_needed = ask_for_features()

	
	os.system('spark-submit --master local[*] classifiers_for_stocks.py %s %s %s %s %s %s' % 
		(method_needed, stock_file, company, output_model, features_needed, output_needed))

	print "CLASSIFICATION DONE!!!"

if __name__ == "__main__":
	if (len(sys.argv) != 4):
		print "Please provide a stock file to be read and output directory with name output and stock name" + \
		"\nUsage: \"python call_classifiers_for_stocks.py input_files/goog.csv_merged.csv output Google\""
		sys.exit(1)
	shutil.rmtree("output", ignore_errors=True)
	main()