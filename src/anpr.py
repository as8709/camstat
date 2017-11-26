import openpyxl
import psycopg2 as psy
from psycopg2 import sql
import datetime
import glob
import os
import argparse
import re

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
CLASS_COLUMN_INDEX = 2
TOTAL_TIME_COLUMN_INDEX = 3

class DataSearcher(object):
    def __init__(self, dbname, db_password):
        self.conn = psy.connect("dbname={} password={}".format(dbname, db_password))
        self.sites = self.get_sites()

    def get_sites(self):
        cur = self.conn.cursor()
        cur.execute("SELECT tablename FROM pg_catalog.pg_tables WHERE tablename SIMILAR TO 's[0-9][0-9]_*\_%';")
        return [site_name for (site_name, ) in cur]

    def search_journeys(self, start, end, via=[], start_time=None, end_time=None, indirect_allowed=True):
        cur = self.conn.cursor()

        route_regex = self.make_route_regex(start, end, via, indirect_allowed)
        sites = [start, end] + via
        for site in sites:
            if ("s" + site) not in self.sites:
                raise Exception("unkown site:{}".format(site))

        site_queries = [sql.SQL("SELECT * from {}").format(sql.Identifier("s"+site)) for site in sites if site is not None]
        site_filter = sql.SQL(" INTERSECT ").join(site_queries)

        cur.execute(sql.SQL("SELECT * from journeys where journey_id in ({})").format(site_filter))
        # TODO do filtering (coarse and fine) on date
        return [self.extract_route(row, start, end, via, indirect_allowed) for row in cur if re.search(route_regex, row[CHAIN_COLUMN_INDEX])]

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

    def extract_route(self, row, start, end, via, indirect_allowed=True):
        '''
        Assuming this row contains at least one matching sub match_route
        Return the equivalent row for just that route
        '''
        return row

    def routes_stats(self, rows):
        '''
        given the result of a query get some statistics on it
        '''
        n_journeys = len(rows)

        veh_classes = [row[CLASS_COLUMN_INDEX] for row in rows]
        class_summary = {}
        for veh_class in set(veh_classes):
            n_class = len([c for c in veh_classes if veh_class == c])
            class_summary[veh_class] = n_class / n_journeys * 100
        trip_times = [row[TOTAL_TIME_COLUMN_INDEX] for row in rows]

        average_trip_time = sum(trip_times, datetime.timedelta())/len(trip_times)

        return (n_journeys, class_summary, average_trip_time)


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
