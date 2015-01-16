#
# sets up the tables etc.
# run this to populate mailing table with data
# 
# you can keep adding bulk entries by running this
# script over and over again.
#
# edit POPULATE_TABLE_N and NUM_DOMAINS to modify
# the number of entries to add and the number of unique
# domains, respectively.
#

import python_mysql as pysql
import conf
import random
import uuid
from datetime import date, timedelta

# I chose not to use CREATE TABLE IF NOT EXISTS since I want
# my subroutine to catch the error where the table already exists.
MAILING_TABLE_NAME = 'mailing'
MAILING_TABLE = 'CREATE TABLE ' + MAILING_TABLE_NAME + '( \
        addr VARCHAR(255) NOT NULL \
        );'

COUNT_TABLE_NAME = 'counts'
COUNT_TABLE = 'CREATE TABLE ' + COUNT_TABLE_NAME + '( \
        id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY, \
        domain VARCHAR(255) NOT NULL, \
        date DATE NOT NULL, \
        count INT(11) UNSIGNED NOT NULL);'

# populates table with N entries
POPULATE_TABLE_N = 100000
# number of unique domains
NUM_DOMAINS = 1000 
# Do we populate the counts table?
POPULATE_COUNTS_TABLE = False
POPULATE_COUNTS_N = 1000

# to ensure we have a unique address, we'll set
# the name of the address using uuid
def populate_mailing_table(con, table):
    for i in range(0, POPULATE_TABLE_N):
        rand_domain = str(random.randint(1, NUM_DOMAINS))
        rand_name = str(uuid.uuid4())
        pysql.mysql_insert(con, table, ['addr'],
                [rand_name+'@em'+rand_domain+'l.com'],
                False)

    # write to disk at the end
    con.commit()


# populates with the same domain name, but with inreasing day.
def populate_count_table(con, table):
    d = date.today() - timedelta(days=POPULATE_COUNTS_N)
    for i in range(0, POPULATE_COUNTS_N):
        pysql.mysql_insert(con, table, ['domain', 'date', 'count'],
                ['email.com' , str(d+timedelta(days=i)), i],
                False)

    # write to disk
    con.commit()
     

def setup():
    # open connection.
    print ('Connecting to DB...')
    con = pysql.mysql_connect(conf.SERVER_CONF)
    # create tables
    print ('Creating the mailing table (this could take awhile)...')
    pysql.mysql_create_table(con, MAILING_TABLE)
    # populating mailling table
    print ('Populating mailing table...')
    populate_mailing_table(con, MAILING_TABLE_NAME)
    # populate counts table if enabled
    if POPULATE_COUNTS_TABLE:
        print ('Creating and populating counts table...')
        pysql.mysql_create_table(con, COUNT_TABLE)
        populate_count_table(con, COUNT_TABLE_NAME)
    # close connection
    print ('Closing connection...')
    pysql.mysql_disconnect(con)



