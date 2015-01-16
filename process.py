# sub-routines for processing data from the databases
# 
# Some notes and assumptions:
# - It is possible to avoid processing all 10 million entries
# in the table if we assume entries or rows will not be 
# removed from the table.  We can do this by simply remembering
# the last row count that was entered and structuring our select
# query with the last row count as the offset.  We will still have
# to process the entire table when we run the script for the first
# time.
#
# - Assumption: Items will not be removed from the mailing table.
# If items are removed, we can check to see if the saved row count
# and the current row count of the mailing table is the same.  If
# the current row count is less, then we can assume something got
# removed.  However!  This does not account for multiple deletions
# and additions that result in the same counts.
#
# - If a table is already populated, it is probably a bad idea
# to construct a select query that returns all entries in the
# table (that's 10 million entries or more!).  In that case,
# we'll have to process the table in chunks.  For this implementation,
# I chose every 1000 rows.
#
# - Assumption: All mailing addresses entered into the mailing table
# are unique.  We don't check for duplicates.  It is possible to 
# implement checking for duplicates by using a hash table to store
# the mailing address, but then we'll have to go through every entry
# in the table again, which will ramp up the time complexity.
#

import re
import python_mysql as pysql
from datetime import date, timedelta

COUNTS_FILE = 'last_mailing_count'
SAVED_ROW_COUNT = 0 

DOMAIN_TOP50_COUNT = {}
DOMAIN_TOP50_PAST_COUNT = {}
DOMAIN_TOP50_GROWTH = {}
DOMAIN_TOP50_DATE = {}

# quick and simple regexp to check for valid email address
# valid email addres contains one @ with characters on either side
def check_valid_email(email):
    regexp = re.compile(r'[^@]+@[^@]+\.[^@]+')
    if regexp.match(email):
        return True
    else:
        return False


# split email address between characters in front of @ and
# characters behind @.  This returns a list of two items,
# so return the second item in the list as it is the domain.
def get_domain(email):
    domain = email.split('@')[1]
    return domain


# updates the COUNTS_FILE with the new count.
def update_count(count):
    f = open(COUNTS_FILE, 'w+')
    f.write(str(count))
    f.close()


# returns +1 if table has been updated (table_count > prev_count)
# returns 0 if nothing has changed (table_count == prev_count)
# returns -1 if items have been removed (table_count < prev_count)
# For the last case, you should go through the entire table again!
def check_table_updated(con, table, save):
    try:
        f = open(save, 'r')
    except IOError:
        # no file, so let's create that file.
        f = open(save, 'w+') 
        f.write(str(0));
        f.close()
        return -1
    table_count = pysql.mysql_count_rows(con, table)
    prev_count = int(f.readline())
    SAVED_ROW_COUNT = prev_count
    f.close()

    if table_count > prev_count:
        return 1
    elif table_count == prev_count: 
        return 0
    else: # table_count < prev_count
        return -1


def write_count_to_table(con, table, domain, date, count):
    # check to see if an entry matches this.
    entry = pysql.mysql_select(con, table, ['id', 'count'],
            "WHERE domain = '" + domain + "' AND date = '" + str(date) +
            "' ORDER BY id DESC LIMIT 1;")
    
    # doesn't exist.  we can directly insert.
    if len(entry) == 0:
        pysql.mysql_insert(con, table, ['domain', 'date', 'count'],
                [domain, str(date), count], False)
    else:
        # update the latest entry.
        domain_id = entry[0][0]
        pysql.mysql_update(con, table, ['count'], [count],
                'WHERE id = ' + str(domain_id) + ';', False)

 

# This is the main subroutine for reading from the
# mailing table and writing to the out_table.  Well,
# that is not exactly true.  What this does first is it 
# reads n entries from in_table until the end.  For every
# n entries read, it will save the domain and the associated
# count.  After saving n entries, it will update the appropriate 
# count and update or insert into out_table.  It skips over entries
# that were already looked at.
def process_n_address(con, in_table, out_table, n, off=0):
    # use a hash table to keep track of domains found.
    domains_searched = {}

    total_rows = pysql.mysql_count_rows(con, in_table)
    if total_rows > n:
        max_iter = (total_rows % n) + 1
    else:
        max_iter = 1
    offset = off
    today = date.today()
    for i in range(0, max_iter):
        rows = pysql.mysql_select(con, in_table, ['addr'],
                'LIMIT ' + str(offset) + ', ' + str(n) + ';')

        for r in rows:
            email = r[0]
            if not check_valid_email(email):
                continue 
            domain = get_domain(email)

            # update the domains_searched hash table
            if domain in domains_searched:
                continue;
            else:
                domains_searched[domain] = 1;

            # get mysql server to give you the count.
            count_query = pysql.mysql_select(con, in_table, ['count(addr)'],
                    "WHERE addr LIKE '%@" + domain + "';")
            count = count_query[0][0]
            
            # write out the hashtable values back into the database.
            # if an entry already exists in the table, that is domain
            # date are the same (date being today), then update instead
            # of insert a new entry.  This is to protect from having
            # multiple entries if this script happens to run more than once
            # in the same day.
            write_count_to_table(con, out_table, domain, today, count)


        # finally, reset DOMAIN_HT and increment offset
        con.commit() # reduce disk writing to only every thousand entries.
        offset += n


# fills in values needed to calculate growth
def get_domain_top50_by_count(con, table):
    entry = pysql.mysql_select(con, table, 
        ['domain', 'MAX(date) as latest', 'MAX(count) as highest_count'],
        'GROUP BY domain ORDER BY highest_count DESC LIMIT 50')

    for e in entry:
        domain = e[0]
        date = e[1]
        count = e[2]
        DOMAIN_TOP50_COUNT[domain] = count;
        DOMAIN_TOP50_DATE[domain] = date;
    

# fills in the past value
def get_last_30_day_count(con, table, domain, date):
    entry = pysql.mysql_select(con, table, ['count'],
            "WHERE domain = '" + domain + "' AND date >= '" + str(date - timedelta(days=30))
            + "' ORDER BY date LIMIT 1;")

    # if there's no such entry, then default to 0
    if len(entry) == 0:
        DOMAIN_TOP50_PAST_COUNT[domain] = 0
    else:
        count = entry[0][0]
        DOMAIN_TOP50_PAST_COUNT[domain] = count


# calculation for growth
def calc_growth(past, present):
    return ((present - past) / past) * 100


# returns a sorted dictionary by value in descending order of the
# domains and associated growth
def get_top50_by_count_sorted_by_growth(con, table):
    get_domain_top50_by_count(con, table)
    today = date.today()
    for domain in DOMAIN_TOP50_COUNT:
        domain_date = DOMAIN_TOP50_DATE[domain]
        get_last_30_day_count(con, table, domain, domain_date)
        # calculate growth
        # the latest date obtained from the table needs to be within 30 days of today
        # or it is not relevant.
        if today - timedelta(days=30) <= domain_date:
            growth = calc_growth(DOMAIN_TOP50_PAST_COUNT[domain], DOMAIN_TOP50_COUNT[domain])
        else:
            growth = 0
        
        # store growth
        DOMAIN_TOP50_GROWTH[domain] = growth

    # return sorted growth as dictionary.
    retval = sorted(DOMAIN_TOP50_GROWTH, key=DOMAIN_TOP50_GROWTH.get, reverse=True)
    return retval


def print_top50(con, table):
    print ('Domain  Count (Growth % in past 30 days)')
    print ('---------------------------------------')
    top50_dict = get_top50_by_count_sorted_by_growth(con, table)
    for k in top50_dict:
        print (k + " ", DOMAIN_TOP50_COUNT[k], '(' + str('{0:.2f}'.format(DOMAIN_TOP50_GROWTH[k])) + '%)')

    
    



