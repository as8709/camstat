import openpyxl
import psycopg2 as psy
from psycopg2 import sql
import datetime
import glob
import os
import argparse
import re
import csv
import functools

from . import filters
from . import groups
from . import stats


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

def compose(functions):
    return functools.reduce(lambda f, g: lambda x: f(g(x)), functions, lambda x: x)

class DataSearcher(object):
    def __init__(self, dbname, db_password, filter_lst=[], group_lst=[], stats_lst=[]):
        self.conn = psy.connect("dbname={} password={}".format(dbname, db_password))
        for fil in filter_lst:
            assert(isinstance(fil, filters.FilterBase))
        self.filters = filter_lst
        #compose all the fine pass filters into one function
        #TODO check that the order is preserved
        self.fine_pass = compose([fil.fine_pass for fil in filter_lst])

        for group in group_lst:
            assert(isinstance(group, groups.GroupBase))
        self.group = compose([group.group for group in group_lst])

        for stat in stats_lst:
            assert(isinstance(stat, stats.BaseStats))
        self.stats = stats_lst

    def get_and_filter(self):
        '''
        Go to the DB and apply the filters
        '''
        cur = self.conn.cursor()
        sql_filters = sql.SQL(" AND ").join([fil.coarse_pass() for fil in self.filters])
        if self.filters:
            cur.execute(sql.SQL("SELECT * from journeys where {};").format(sql_filters))
        else:
            #no filter means no WHERE
            cur.execute("SELECT * from journeys;")
        return self.fine_pass(cur)

    def combined(self):
        '''
        get the results from the db, apply the filters,
        group then get the statistics for each group of rows
        '''
        groups = self.group(list(self.get_and_filter()))
        return self.apply_stats(groups)

    def stat_headers(self):
        out = []
        for stat in self.stats:
            out += stat.stat_descriptions()
        return out

    def apply_stats(self, group_or_rows):
        if isinstance(group_or_rows, list):
            stat_lists = [stats.make_stats(group_or_rows) for stats in self.stats]
            return [stat for sublist in stat_lists for stat in sublist]
        elif isinstance(group_or_rows, dict):
            return {key: self.apply_stats(value) for key, value in group_or_rows.items()}
        else:
            raise Exception("Unknown group type:{}".format(type(group_or_rows)))


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
