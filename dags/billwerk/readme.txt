Based on the current code setup, 
all the data is fetched first (up to the specified row limit)
, and only after that entire batch has been fetched 
, is it then split into smaller chunks and inserted into BigQuery.

If you have concerns about memory utilization, 
especially when dealing with a large number of rows, 
you might need to reconsider this design. 
You can potentially split the data-fetching 
and data-inserting processes into smaller, 
more frequent operations rather than fetching a very large batch at once.