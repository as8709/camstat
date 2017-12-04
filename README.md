# camstat
Library for creating a database from an ANPR dataset then providing an API into it

Initial Configuration
---------------------
The first step in analysis is to convert the excel data from the spreadsheets found at
http://opendata.cambridgeshireinsight.org.uk/dataset/greater-cambridge-anpr-data-trip-chain-reports
into a postgres database 
1) Install postgres if you haven't already
2) Create an empty database
3) Put the spreadsheets into a directory without any other .xlsx files
4) python make_db.py <xlsx dir> <db name> <db password> -c
  This will create an empty journeys table in the given database then
  load the data from the spreadsheets into the databse.
  This may take a couple of hours
  
Database structure
------------------
The database consists of a *journeys* table which contains the actual data on the journeys (start time, vehicle class etc.)
and a number of *site* tables, one per combination of camera site and direction. Each site table contains the *journey id* of each journey that includes the site and direction at least once.

The site tables are used to allow a first pass of filtering by route in SQL. The **INTERSECTION** of all the site tables that a route contains narrows down the number of a journeys a more complex filter such as a regex needs to work on. Note that the site tables can't discriminate on the order the sites where visited in.

Searching API
-------------
The entry point to the searching API is camstat.DataSearcher().
This class is passed (along with the db name and password) lists of the **filters**, **groups** and **stats** to use on the data
Each list consists of objects inheriting from the base class defined in the correspondingly named module under camstat

If you don't want to implement your own filters groups and stats several are predefined alongside the base class for each module

#### Filters
Filters are used to select a subset of all the recorded journeys according to some criteria
Each Filter class must inherit from *camstat.filters.FilterBase()*.
The API is a *coarse_pass* which is used in the SQL call to get the journeys and a *fine_pass* that post processes the output rows from the SQL query

#### Groups
Groups object group the journeys by into lists of journeys which match some criteria.
The output is a Python dictionary with keys for each group of journeys
Groups can be nested i.e. the output of the grouping stage can be a dictionary of dictionaries and so on until you get to a list of matching rows as the base case

### Stats
The stats class takes a list of journey entries and returns a list of statistics on them. The output of multiple stats classes
are concatenated together to give the final list of statistics for the journeys.
Note that the grouping from the grouping stage is preserved but the list of journeys for each group is replaced with a list of statistics of that list.
The *stat_description* function return a list of human readable descriptions for each of the statistics the class generates

## Usage
To get the results of running the **filters**, **groups** and **stats** stages call .combined() on the DataSearcher object




