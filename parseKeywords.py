#call with scriptName.py dataFile.csv outputFileName
#given a csv with dates, titles, and scores, output a csv with an additional column that indicates keywords, as well as a csv with keyword and occurence count per day 
#   use the findKeyword.py function for identifying keywords (returns ordered array of (title,score))
#   use the blacklist.txt file for keywords to blacklist from identification (\n delimited list of words that should be ignored)

import findKeyword #findKeyword.find_keyword(title) is intended usage
import sys
from operator import itemgetter
import string

reload(sys)
sys.setdefaultencoding('utf8')

from multiprocessing import Process, Queue, Manager


def multiprocessRecords(records,nprocs):
	#given an array of records (date, title, score) to process and number of processes to spin up, return an array with updated records and dict of keywrod counts

	def worker(records, record_queue, keyword_queue, i):
		#worker function that generates dicts for its chunk of work
		recordArray = []
		keywordDict = {}

		for record in records:
			recordArray


def main():

	#load blacklist
	blackListFile = open('blacklist.txt')
	blackList = blackListFile.read()
	blackListWords = blackList.split() #create a list of words from the black list file

	print "Blacklist file loaded!"
	

	#load dataset
	with open(sys.argv[1], 'r') as f:
		f.readline() #dump the header line
		dataSet = [] #create an array to contain all records
		for line in f:
			record = line.split(',')
			dataSet.append((record[0],record[1],record[2])) #date, title, score

	print "Dataset loaded"

	keywordDict = {} #create a dict that tracks keyword counts per day: key is date, dict with keyword key and frequency value is value

	#iterate over each title in the data set and identify keywords
	for record in dataSet:
		date = record[0]
		title = record[1]

		print "Processing record " + str(title) + " from " + str(date)

		#prep dict for date
		if date not in keywordDict:
			keywordDict[date] = {}

		#eliminate punctuation from title
		title = title.translate(None, string.punctuation)
		
		#call findKeyword to get list of keyword,score
		results = findKeyword.find_keyword(title)
	
		#iterate over keyword,score list to find all keywords with the top score
		maxValue = max(results, key=itemgetter(1))[1] 
		keywords = []
		for keyword,score in results:
			if score == maxValue: #get keywords with the max score
				
				keyword = keyword.translate(None,string.punctuation) #eliminate any punctuation keywords

				if keyword not in blackListWords: #check if keyword is not in the blacklist
					print "Adding keyword for this record: " + str(keyword)
					keywords.append(keyword)

					#then add to keyword dict (or update record with increment)
					if keyword in keywordDict[date]:
						keywordDict[date][keyword] += 1
					else:
						keywordDict[date][keyword] = 1
	
		#store record with new keyword
		record = (record[0],record[1],record[2],keywords) #date, title, score, keywords array 
		print "Record is now " + str(record)

	print "Now outputing files"

	#init output file names
	outputFileNameAugmented = str(sys.argv[2]) + "_Augmented.csv"
	outputFileNameAggregated = str(sys.argv[2]) + "_Aggregated.csv"
	
	#output datasets to csv with all original records and additional column with keywords, and new aggregation 
	with open(outputFileNameAugmented, 'w') as f:
		f.write("Date, Title, Score, Keywords \n") #write header rows

		for record in dataSet:
			date = str(record[0]).replace(',','')
			title = str(record[1]).replace(',','')
			score = str(record[2]).replace(',','')
			keywords = '|'.join(record[3]) #create a pipe delimited list of keywords
			
			outputStr = ','.join((date,title,score,keywords)).encode('utf-8')
			outputStr = ' '.join((outputStr, '\n')).encode('utf-8')
			f.write(outputStr)

	#use keywordDict to generate csv with date, keyword, occurenceCount

	with open(outputFileNameAggregated, 'w') as f:
		f.write("Date, Keyword, OccurenceCount \n") #write header rows

		for date in keywordDict:
			#iterate over each day's dictionary
			dateDict = keywordDict[date]
			for keyword in dateDict:
				#iterate over each keyword in each day's dictionary
				dateStr = str(date).replace(',','')
				name = str(keyword).replace(',','')
				count = str(dateDict[keyword]).replace(',','')
				outputStr = ','.join((dateStr,name,count)).encode('utf-8')
				outputStr = ' '.join((outputStr, '\n')).encode('utf-8')
				f.write(outputStr)

main()


