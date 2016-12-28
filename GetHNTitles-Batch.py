import azure.storage.blob as azureblob
import argparse
import collections
import os
import string 

import sys
#fix for output encoding error
reload(sys)  
sys.setdefaultencoding('utf8')

from operator import itemgetter

import multiprocessing
import csv
import datetime
import math
import time
from multiprocessing import Process, Queue, Manager

from hackernews import HackerNews
hn = HackerNews() 


def binSearch(inputDateTime):
	#find the min item ID for a given datetime 	

	#print 'performing bin search for ' + str(inputDateTime) #debug 

	lowEnd = 0
	
	while True:
		try:
		     highEnd = hn.get_max_item() #use max item ID as high end
		     break
		except:
		     print "Max item failed"
		     time.sleep(5) #sometimes http requests fail, so wait and retry 

	while highEnd - lowEnd > 1:
		#target is to get highEnd to be one away from lowEnd
		#lowEnd must always have a submission time less than inputDateTime
		#highEnd must always have a submission time >= inputDateTime
		
		mid = (highEnd + lowEnd)/2 #set mid point to middle of high and low
		
		while True:
			try:
		     	     midSubTime = hn.get_item(mid).submission_time
			     break
			except:
		             print "Failed due to mid point: " + str(mid)
			     mid = mid + 1 #increment and try next item

		if midSubTime < inputDateTime:
			lowEnd = mid
		else:
			highEnd = mid

	return highEnd 

def itemRange(inputDateTime):
	#binary search to find item ID for a given date
	#inputDate should be a datetime 

	lowEnd = binSearch(inputDateTime)
	highEnd = binSearch(inputDateTime + datetime.timedelta(days=1))

	highEnd = highEnd - 1 #since binSearch gets lowest item id for next day

	return (lowEnd,highEnd)

def GetRecord(itemId):
	#given an item ID, return a tuple of (score,title) 
	try:
		item = hn.get_item(itemId)
		
		if item.item_type == "story": #filter out non-stories 
			title = item.title
			score = item.score 
			
			return (score,title)
		
		else:
			return (0, "Non story") 

	except:
		return (0, "Null Record") 

def multiprocessHNTitles(nums, nprocs):
	#given a set of records and number of processes parallelize work and return a dict 

	def worker(nums, out_q, i):
		#worker function to construct a dictionary per chunk and add to an output queue
		outdict = {}
		for n in nums:
			#print "Processing record" + str(n)
			outdict[n] =  GetRecord(n) #dict record has item # as key, tuple of (score,title) as value

		#print "Worker " + str(i) + " has completed"
		out_q.put(outdict)

	#each process will get chunksize of item numbers and a queue to put output dict into

	manager = Manager()
	out_q = manager.Queue()
	chunksize = int(math.ceil(len(nums)/float(nprocs)))
	procs = [] 

	for i in xrange(nprocs):
		p = Process(
			target=worker,
			args=(nums[chunksize*i:chunksize*(i+1)],
			out_q, i))
		procs.append(p)
		p.start()

	#wait for worker processes to finish
	for p in procs:
		p.join()

	#collect results into a single dict then output the dict
	#print "constructing dictionary now" 
	resultdict = {}
	for i in xrange(nprocs):
		resultdict.update(out_q.get())

#	print str(resultdict)
	return resultdict

	

def GetTopNTitlesForDay(inputDate, N):

	#returns a list of tuples of top N titles and their scores 
	#input is a datetime and integer

	#get item id range
	itemTuple = itemRange(inputDate) 

	#init vars for dict construction

	#print str(itemTuple[0]) + 'is min item number for' + str(inputDate) + ' and ' + str(itemTuple[1]) + ' is max item number' #debug statement

	current = itemTuple[0]
	maxNum = itemTuple[1]

	#instantiate dicts and arrays for items, scores, and titles
	itemDict = {}
	scores = []
	outputList = []

	#construct dictionary in parallelized process
	itemDict = multiprocessHNTitles(range(current,maxNum), 512) #returns dict with itemNum as key, (score,title) tuple as value

	
	#construct score list to trim dictionary
	for key in itemDict:
#		print "appending score for key " + str(key) + " with value " + str(itemDict[key])
		scores.append(itemDict[key][0])


#	print 'dict construction complete, starting sort and clean up for ' + str(inputDate) #debug statement 

	#sort score list to get score of top N stories 
	scores.sort(reverse=True)
	del scores[N:] #truncate list to only top N scores 
#	print "top score list is: " + str(scores) #debug
	
	#trim dictionary to top N

	for key in itemDict:
		if itemDict[key][0] >= min(scores): #if score is geq than Nth greatest score 
			outputList.append((itemDict[key][1],itemDict[key][0])) #add this (title,score) to the list

	#sort list of tuples 
	outputList.sort(key=itemgetter(1), reverse=True)

	return outputList



def WriteDataSet(data, date, storageaccount, storagecontainer,sastoken):
	#this ouputs a csv using a list of (title,score) tuples and a date 
	#  output csv is named for date 

	outputName = str(date) + ".tsv"

	with open(outputName, 'w') as csv_file: #open file in write mode

		csv_file.write("Date \t Title \t Score \n") #write header rows

		for record in data: #iterate over each title,score in the list
			outputStr = '\t'.join((date,str(record[0]),str(record[1]))).encode('utf-8') #create record and encode as utf-8	
			outputStr = ' '.join((outputStr, '\n')).encode('utf-8') #add endline
			csv_file.write(outputStr)


	blob_client = azureblob.BlockBlobService(account_name=storageaccount, sas_token=sastoken)

	output_file_path = os.path.realpath(outputName)

	print 'Uploading file ' + str(output_file_path) + ' to container ' + str(storagecontainer)

	blob_client.create_blob_from_path(storagecontainer,outputName, output_file_path)

def main():
	#this script is called with a series of arguments from the batch client
	# topN, minDate, maxDate, storageaccount, storagecontainer, sastoken

	parser = argparse.ArgumentParser()

	parser.add_argument('--topN', required=True,help='Top N is number of top posts to grab for each day')
	parser.add_argument('--minDate', required=True, help='start date of range in yyyy-mm-dd')
	parser.add_argument('--maxDate', required=True, help='end date of range in yyyy-mm-dd')
	parser.add_argument('--storageaccount', required=True, help = 'Storage account name')
	parser.add_argument('--storagecontainer', required=True, help = 'Storage container to which upload results')
	parser.add_argument('--sastoken', required=True, help='SAS token for write access to storage container') 

	args = parser.parse_args()

		
	minDate = datetime.datetime.strptime(args.minDate, "%Y-%m-%d")
	maxDate = datetime.datetime.strptime(args.maxDate, "%Y-%m-%d")
	n = int(args.topN)

	#iterate over days 	
	currentDate = minDate

	while currentDate <= maxDate:
		#call topN for each day in range
		
		dateStr = currentDate.date().isoformat() #gets date of datetime then prints it as yyyy-mm-dddd

		print "Getting top N titles for " + dateStr #debug statement

		WriteDataSet(GetTopNTitlesForDay(currentDate,n), dateStr, args.storageaccount, args.storagecontainer, args.sastoken) #get top N titles for day as list and pass to writeDataSet with the dataStr

		currentDate = currentDate + datetime.timedelta(days=1) #increment for next day 


	
#call main 
main()
