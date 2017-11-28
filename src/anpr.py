import openpyxl
import psycopg2 as psy
from psycopg2 import sql
import datetime
import glob
import os
import argparse
import re
import csv

CAMERA_START=1
CAMERA_END=97
DATA_START_ROW = 12

def make_journeys_table(dbname, password):
    conn = psy.connect("dbname={} password={}".format(dbname, password))
    cur = conn.cursor()
    cur.execute("CREATE TABLE journeys (journey_id SERIAL PRIMARY KEY"+
    ", timestamp timestamp"+
    ", class text"+
    ", total_trip_time interval"+
    ", chain text"+
    ", trip_destinations_and_time text"+
    ", journey_end_time timestamp);"
    )
    conn.commit()


class DataLoader(object):
    def __init__(self, wb, dbname, password):
        self.wb = wb
        self.conn = psy.connect("dbname={} password={}".format(dbname, password))

    def load(self):
        for camera_name in self.get_camera_names():
            print("loading camera:{}".format(camera_name))
            try:
                ws = self.wb[camera_name]
            except KeyError:
                ws = None
            if ws:
                [self.load_journey(row[1:6]) for row in ws.iter_rows(min_row=DATA_START_ROW)]

    def get_camera_names(self):
        camera_names = ["{:02d}".format(camera) for camera in range(CAMERA_START, CAMERA_END) if camera not in [35, 5, 15, 39, 40, 51, 88]]
        return camera_names +  ["35A", "35B"]

    def load_journey(self, row):
        '''
        load the given journey entry into the database
        '''
        if row[0].value is not None:
            journey_id = self.add_journey_entry(row)
            for site in set(row[3].value.split(">")):
                if not self.table_exists("s"+site):
                    self.create_site_set_table("s"+site)
                self.add_to_site_set("s"+site, journey_id)

    def add_journey_entry(self, row):
        cur = self.conn.cursor()
        trip_time = datetime.timedelta(minutes=row[2].value)
        if type(row[0].value) == str:
            end_time = datetime.datetime.strptime(row[0].value,"%d/%m/%Y %H:%M:%S") + trip_time
        else:
            end_time = row[0].value + trip_time
        cur.execute("INSERT INTO journeys (timestamp, class, total_trip_time, chain, trip_destinations_and_time, journey_end_time)" +
        "VALUES (%s, %s, %s, %s, %s, %s) RETURNING journey_id;", [row[0].value,
        row[1].value, trip_time, row[3].value, row[4].value, end_time])
        journey_id = cur.fetchone()

        self.conn.commit()
        return journey_id

    def table_exists(self, table_name):
        cur = self.conn.cursor()
        cur.execute("select relname from pg_class where relname = %s;", [table_name])
        if cur.fetchone():
            return True
        else:
            return False

    def create_site_set_table(self, site):
        cur = self.conn.cursor()
        s = sql.SQL("CREATE TABLE {} (journey_id serial UNIQUE REFERENCES"+
        " journeys);").format(
            sql.Identifier(site))
        cur.execute(s)
        self.conn.commit()

    def add_to_site_set(self, site, journey_id):
        cur = self.conn.cursor()
        s = sql.SQL("INSERT INTO {} (journey_id) VALUES (%s);").format(sql.Identifier(site))
        cur.execute(s, [journey_id])

        self.conn.commit()

CHAIN_COLUMN_INDEX = 4
CHAIN_TIME_COLUMN_INDEX = 5
CLASS_COLUMN_INDEX = 2
TIMESTAMP_COLUMN_INDEX = 1
TOTAL_TIME_COLUMN_INDEX = 3

class DataSearcher(object):
    def __init__(self, dbname, db_password):
        self.conn = psy.connect("dbname={} password={}".format(dbname, db_password))
        self.sites = self.get_sites()

    def get_sites(self):
        cur = self.conn.cursor()
        cur.execute("SELECT tablename FROM pg_catalog.pg_tables WHERE tablename SIMILAR TO 's[0-9][0-9]_*\_%';")
        return [site_name for (site_name, ) in cur]

    def search_journeys(self, start, end, via=[], start_time=None, end_time=None, indirect_allowed=True, filter_classes=None):
        cur = self.conn.cursor()

        route_regex = self.make_route_regex(start, end, via, indirect_allowed)
        sites = [start, end] + via
        for site in sites:
            if ("s" + site) not in self.sites:
                raise Exception("unkown site:{}".format(site))

        site_queries = [sql.SQL("SELECT * from {}").format(sql.Identifier("s"+site)) for site in sites if site is not None]
        site_filter = sql.SQL(" INTERSECT ").join(site_queries)
        if filter_classes is None:
            class_filter = sql.SQL("")
        else:
            class_filter = sql.SQL("AND (class in %s)")

        if filter_classes is None:
            cur.execute(sql.SQL("SELECT * from journeys where (journey_id in ({})) {}").format(site_filter, class_filter))
        else:
            cur.execute(sql.SQL("SELECT * from journeys where journey_id in ({}) {}").format(site_filter, class_filter), [tuple(filter_classes)])
        # TODO do filtering (coarse and fine) on date
        row_lists = [self.extract_route(row, route_regex) for row in cur if re.search(route_regex, row[CHAIN_COLUMN_INDEX])]
        #there could be multiple matches (and therefore output rows) per journey so extract_route
        # returns a list of lists, flatten this list
        return [x for rows in row_lists for x in rows]


    def make_route_regex(self, start, end, via, indirect_allowed):
        site_regex = r"(\d\d\D?_([NESW]|(OUT)|(IN))>)"
        if (start == end and via == []):
            route_regex = r"{start}.*".format(start)
        elif not indirect_allowed:
            via_regex = ">".join(via)
            route_regex = start + ">" + via_regex + ">" + end
        else:
            via_regex = "".join([site_regex+"*"+site + ">" for site in via])
            route_regex = start + ">" + via_regex + site_regex + "*" + end
        return route_regex

    def extract_route(self, row, route_regex):
        '''
        Assuming this row contains at least one matching sub match_route
        Return the equivalent row for just that subroute, changing the times accordinglu
        '''
        matched_chains = re.findall("("+route_regex +")", row[CHAIN_COLUMN_INDEX])
        out_rows = []
        for matched_chain in matched_chains:
            if type(matched_chain) == tuple:
                matched_chain = matched_chain[0]
            first_site = matched_chain.split(">")[0]

            #find how many sites into the chain the match starts
            match_start = row[CHAIN_COLUMN_INDEX].find(matched_chain)
            match_chain_index_start = row[CHAIN_COLUMN_INDEX][:match_start].count(">")

            match_chain_index_end = match_chain_index_start + len(matched_chain.split(">")) - 1
            # the chain with the times misses the first site in the CHAIN_COLUMN_INDEX
            # add it back in
            time_chain = (first_site + "(0.0)" + row[CHAIN_TIME_COLUMN_INDEX]).split(">")

            start_time_offset = self.get_time_offset_from_time_chain(time_chain, match_chain_index_start)
            end_time_offset = self.get_time_offset_from_time_chain(time_chain, match_chain_index_end)

            start_time_offset = datetime.timedelta(minutes=start_time_offset)
            end_time_offset = datetime.timedelta(minutes=end_time_offset)

            start_time = row[TIMESTAMP_COLUMN_INDEX] + start_time_offset
            end_time = row[TIMESTAMP_COLUMN_INDEX] + end_time_offset

            new_time_chain = ">".join(time_chain[match_chain_index_start:(match_chain_index_end+1)])
            new_row = (row[0], start_time, row[CLASS_COLUMN_INDEX], end_time-start_time, matched_chain, new_time_chain, end_time)
            out_rows.append(new_row)
        return out_rows

    def get_time_offset_from_time_chain(self, time_chain, chain_index):
        '''
        Given a time chain and an index to a given entry
        parse the time differences assuming each entry is
        in the format <site_name>(<time_offset>)
        then return the total time offset up to the given index
        return the time offset
        '''
        entry_regex = r"\d\d\D?_([NESW]|(OUT)|(IN))\((\d+(.\d+)?)\)"
        time_offsets = [float(re.match(entry_regex, entry).group(4)) for entry in time_chain[:(chain_index+1)]]

        return sum(time_offsets)

class DataStats(object):
    '''
    Given a list of rows that matched a query calculate some statistics on them
    '''
    def __init__(self, rows):
        self.rows = rows
        self.routes_stats(rows)
        self.class_summary = {}
        self.trip_times = []
        self.n_journeys = 0
        self.average_trip_time = None


    def serialise(self, entries, filename="data.csv"):
        with open("data.csv", 'w') as csvfile:
            writer = csv.writer(csvfile)
            for entry in entries:
                writer.writerow(entry)

    def __str__(self):
        return "n_journeys:{}\nvehicle class summary (% of total, number):{}\naverage trip time:{}".format(
        self.n_journeys, self.class_summary, self.average_trip_time
        )

    def routes_stats(self, rows):
        self.n_journeys = len(rows)
        if self.n_journeys:
            veh_classes = [row[CLASS_COLUMN_INDEX] for row in rows]
            self.class_summary = {}
            for veh_class in set(veh_classes):
                n_class = len([c for c in veh_classes if veh_class == c])
                self.class_summary[veh_class] = (n_class / self.n_journeys * 100, n_class)
            self.trip_times = [row[TOTAL_TIME_COLUMN_INDEX] for row in rows]

            self.average_trip_time = sum(self.trip_times, datetime.timedelta())/len(self.trip_times)
            return (self.n_journeys, self.class_summary, self.trip_times, self.average_trip_time)
        else:
            return (0, {}, [], None)
    def group_by_start_hour(self):
        '''
        Group the rows by hour
        '''
        groups = {}
        #TODO default dict
        for i in range(24):
            groups[i] = []
        for row in self.rows:
            start_time = row[TIMESTAMP_COLUMN_INDEX]
            groups[start_time.hour].append(row)
        return groups


    def stats_by_hour(self):
        '''
        Collect stats for all journeys when they are grouped by start hour
        '''
        hour_groups = self.group_by_start_hour()
        return {hour: self.routes_stats(rows) for hour, rows in hour_groups.items()}

    def durations_by_hour(self):
        '''
        Collect the average durations of journeys grouped by start hour
        '''
        hour_groups = self.stats_by_hour()
        out = []
        for hour, (n_journeys,_,times,avg_time) in hour_groups.items():
            if n_journeys:
                out.append((hour, avg_time.seconds, max(times).seconds, min(times).seconds))
        return out


if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Script for loading data from anpr spreadsheets into a db")
    parser.add_argument("xlsx_dir", help="path to the directory where the spreadsheets are")
    parser.add_argument("dbname", help="name of the db to create")
    parser.add_argument("password", help="password to the database")
    args = parser.parse_args()
    for spreadsheet in glob.glob(os.path.join(os.path.abspath(args.xlsx_dir), "*.xlsx")):
        wb = openpyxl.load_workbook(filename=spreadsheet, read_only=True)
        DataLoader(wb, args.dbname, args.password).load()
        print("loaded:{}".format(spreadsheet))
