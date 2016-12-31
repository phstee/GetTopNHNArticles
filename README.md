# GetTopNHNArticles
Python script to get top N hacker news articles for a given day 
Batch implementation will output to configured account's blob storage
Single machine implementation will output locally 
Parse script will take output from batch or single-machine implementation and identify keywords per record; also outputs aggregated set of keywords and occurences per day

Configure credFile.py (from credFile.py.default) in order for batch script to work 

Credit for Batch implementation goes to Microsoft's tutorial, including the helper files.
Credit for keyword identification goes to http://agiliq.com/blog/2009/03/finding-keywords-using-python/

Not particularly intended to be reused.
Being used for proof of concept for future project 
No documentation or support available
