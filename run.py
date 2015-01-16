#####################################################################
#                                                           
#  This script will count the number of times a domain
#  appears as an entry and will report the top 50
#  domains by count sorted by procentage growth over the last
#  30 days.
#
#  This script assumes that the tables in the database
#  are populated and exist.  
#
#  If they do not exist, run setup.py.  That will create
#  and populate the tables with data.
#
#  The script will not reprocess the entire mailing table
#  unless it detects a change.  A change is detected with the
#  process.check_table_updated() function that returns +1 if
#  items have been added to the mailing table, 0 if nothing
#  has changed and -1 if items have been removed (refer to
#  the read me and commented sections for notes and assumptions).
#  
#  You can force the script to re-process the entire mailing table
#  by deleting the file defined by process.COUNTS_FILE or by
#  changing the number in that file to soemthing lower than the 
#  number of entries in the mailing table.
#
#####################################################################

import python_mysql as pysql
import conf
import process
import setup

# open connection.
print ('Connecting to DB...')
con = pysql.mysql_connect(conf.SERVER_CONF)

# Create counting table if it doesn't exist just in case.
print ('Creating counts table just in case...')
pysql.mysql_create_table(con, setup.COUNT_TABLE)

# Check to see if the table has been updated, and thus
# we either need to start from the previous saved count or from the beginning.
print ('Processing mailing table (this could take awhile)...')
table_update_state =  process.check_table_updated(con, setup.MAILING_TABLE_NAME, 
        process.COUNTS_FILE)
if table_update_state == 1:
    # start processing from SAVED_ROW_COUNT offset.
    process.process_n_address(con, setup.MAILING_TABLE_NAME, 
            setup.COUNT_TABLE_NAME, conf.PROCESS_N, process.SAVED_ROW_COUNT)
elif table_update_state == 0: # do nothing
    pass
else:
    # re-process from the beginning.
    process.process_n_address(con, setup.MAILING_TABLE_NAME, 
            setup.COUNT_TABLE_NAME, conf.PROCESS_N)

# update the new count.
print ('Updating new count...')
new_count = pysql.mysql_count_rows(con, setup.MAILING_TABLE_NAME)
process.update_count(new_count)

# print report of top 50 by count sorted by growth in the past 30 days.
process.print_top50(con, setup.COUNT_TABLE_NAME)

print ('Closing connectoin')
pysql.mysql_disconnect(con)
