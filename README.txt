Main Script Files:
\_run.py
    \__________ Main script.  Calls subroutines in other files.
\_conf.py
    \__________ Configuration for MySQL database connection
                and script stuff.
\_process.py
    \__________ Contains all the subroutines that do the processing.
\_python_mysql.py
    \__________ Wrapper functions for the MySQL-Python connector.
\_setup.py
    \__________ Configuration for creating tables and also
                contains subroutines for populating tables for
                script testing reasons.
\_last_mailing_count
    \__________ Keeps track of the last count of the number of rows
                in the mailing table.

Files for testing:
\_run_setup.py
    \__________ Wrapper for setup.py.  Runs the subroutines.
\_test_wrapper.py
    \__________ Unit tests for testing the python_mysql.py wrapper.
\_test_process.py
    \__________ Unit tests for process.py routines.
\_greater.txt
    \__________ Input file for testing.

To use:
1.  Edit conf.py to connect to the database.
2.  Run run.py.

This script will count the number of times a domain
appears as an entry and will report the top 50
domains by count sorted by procentage growth over the last
30 days.

This script assumes that the tables in the database
are populated and exist.  

Tables can be populated for testing purposes by running
run_setup.py.  That will create and populate the tables with data.

The script will not reprocess the entire mailing table
unless it detects a change.  A change is detected with the
process.check_table_updated() function that returns +1 if
items have been added to the mailing table, 0 if nothing
has changed and -1 if items have been removed (refer to
the read me and commented sections for notes and assumptions).
  
You can force the script to re-process the entire mailing table
by deleting the file defined by process.COUNTS_FILE or by
changing the number in that file to soemthing greater than the 
number of entries in the mailing table.

The main subroutine for reading from the mailing table and writing 
to the out_table first reads n entries from in_table until the end.  
For every n entries read, it will save the domain and the associated
count.  After saving n entries, it will update the appropriate 
count and update or insert into out_table.  It skips over entries
that were already looked at.

Some notes and assumptions regarding design of script:
- It is possible to avoid processing all 10 million entries
in the table if we assume entries or rows will not be 
removed from the table.  We can do this by simply remembering
the last row count that was entered and structuring our select
query with the last row count as the offset.  We will still have
to process the entire table when we run the script for the first
time.

- Assumption: Items will not be removed from the mailing table.
If items are removed, we can check to see if the saved row count
and the current row count of the mailing table is the same.  If
the current row count is less, then we can assume something got
removed.  However!  This does not account for multiple deletions
and additions that result in the same counts.

- If a table is already populated, it is probably a bad idea
to construct a select query that returns all entries in the
table (that's 10 million entries or more!).  In that case,
we'll have to process the table in chunks.  For this implementation,
I chose every 1000 rows.

- Assumption: All mailing addresses entered into the mailing table
are unique.  We don't check for duplicates.  It is possible to 
implement checking for duplicates by using a hash table to store
the mailing address, but then we'll have to go through every entry
in the table again, which will ramp up the time complexity.


