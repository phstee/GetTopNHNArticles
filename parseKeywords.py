#call with scriptName.py dataFile.csv outputFileName
#given a csv with dates, titles, and scores, output a csv with an additional column that indicates keywords, as well as a csv with keyword and occurence count per day 
#   use the findKeyword.py function for identifying keywords (returns ordered array of (title,score))
#   use the blacklist.txt file for keywords to blacklist from identification (\n delimited list of words that should be ignored)

import findKeyword #findKeyword.find_keyword(title) is intended usage
import sys
from operator import itemgetter
import string
import math

reload(sys)
sys.setdefaultencoding('utf8')

from multiprocessing import Process, Queue, Manager

#global vars 

#load blacklist
blackListFile = open('blacklist.txt')
blackList = blackListFile.read()
blackListWords = blackList.split() #create a list of words from the black list file
print "Blacklist file loaded!"



def multiprocessRecords(records,nprocs):
	#given an array of records (date, title, score) to process and number of processes to spin up, return an array with updated records with (date,title,score,[keywords]); and dict with date as key, value ==(dict(keyword, occurenceCount))

	def mergeKeywordDicts(dict1, dict2):
		#given two keyword dicts with dict(date,dict(keyword,count)), return a combined dictionary with aggregated counts

		outputDict = {}
		
		#iterate over each day of dict 1
		for dateKey in dict1:

			#prep outputDict
			if dateKey not in outputDict:
				outputDict[dateKey] = {}

			try:
				#dict1 and dict 2 have this date key
				dict1DateDict = dict1[dateKey]
				dict2DateDict = dict2[dateKey] 

				#add all keywords from dict 1
				for keyword in dict1DateDict:
					if keyword not in outputDict[dateKey]:
						outputDict[dateKey][keyword] = dict1DateDict[keyword]

				#add all keywords from dict 2 (or merge them in)
				for keyword in dict2DateDict:
					if keyword not in outputDict[dateKey]:
						outputDict[dateKey][keyword] = dict2DateDict[keyword]
					else:
						outputDict[dateKey][keyword] += dict2DateDict[keyword]
			except:
				#dict 1 only has this date key so clone all keywords/counts
				outputDict[dateKey] = dict1[dateKey]

		#any remaining records should be days of dict 2 where no records exist in dict 1
		# so simply clone all keyword/counts for datekeys that are in dict2 but not in the outputdict yet
		for dateKey in dict2:
			if dateKey not in outputDict:
				outputDict[dateKey] = dict2[dateKey]

		return outputDict


	def worker(records, record_queue, keyword_queue, i):
		#worker function that generates output array of updated records given an array of records and an output queue; also outputs a dict of keywords with date, keyword, frequency 
		updatedRecordArray = []
		keywordDict = {}


		for record in records:
			date = str(record[0])
			title = str(record[1])
			score = int(record[2])
			
			print "Processing record " + title + " from " + date + " with score " + str(score)

			#prep keyword dict
			if date not in keywordDict:
				keywordDict[date] = {}

			title = title.translate(None,string.punctuation) #eliminate punctuation from title

			#call findKeyword to get a list of (keyword,score)
			results = findKeyword.find_keyword(title)

			#iterate over (keyword,score) list to find all keywords with the top score
			maxValue = max(results, key=itemgetter(1))[1]
			keywords = []

			for keyword,score in results:
				if score == maxValue: #get keywords with max score
					keyword = keyword.translate(None,string.punctuation) #eliminate punctuation
					if keyword not in blackListWords: #check if keyword not in blacklist
						print "Adding keyword for this record: " + str(keyword)
						keywords.append(keyword)

						#then update keyword dict
						if keyword in keywordDict[date]:
							keywordDict[date][keyword] += 1
						else:
							keywordDict[date][keyword] = 1

			#store record with new keyword list
			newRecord = (date,title, score, keywords)
			updatedRecordArray.append(newRecord)
			print "Record is now " + str(newRecord)

		#now add array to the queue
		record_queue.put(updatedRecordArray)

		#add keyword dict to queue
		keyword_queue.put(keywordDict)

	#main multiprocess function now
	manager = Manager()
	record_queue = manager.Queue()
	keyword_queue = manager.Queue()
	chunksize = int(math.ceil(len(records)/float(nprocs)))
	procs = []

	for i in xrange(nprocs):
		p = Process(
			target=worker,
			args=(records[chunksize*i:chunksize*(i+1)],record_queue, keyword_queue, i))
		procs.append(p)
		p.start()

	#wait for workers to finish
	for p in procs:
		p.join()

	#collect updated records and keyword dicts 
	recordArray = []
	keywordDict = {}
	for i in xrange(nprocs):
		recordArray.extend(record_queue.get())

		keywordDict = mergeKeywordDicts(keywordDict, keyword_queue.get()) 

	return (recordArray, keywordDict)
			


def main():	

	#load dataset
	with open(sys.argv[1], 'r') as f:
		f.readline() #dump the header line
		dataSet = [] #create an array to contain all records
		for line in f:
			record = line.replace('\n','').split(',')

			if len(record) > 3:
				print "BAD RECORD: " + str(record)
			dataSet.append((record[0],record[1],record[2])) #date, title, score

	print "Dataset loaded"

	updatedRecords = []
	keywordDict = {} 

	#call multiprocess function to split work of parsing records and rebuilding dataset with keywords
	updatedRecords, keywordDict = multiprocessRecords(dataSet, 32)


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


