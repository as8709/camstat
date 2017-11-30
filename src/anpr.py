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

import filters


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
    def __init__(self, dbname, db_password, filter_lst=[], group_lst=[]):
        self.conn = psy.connect("dbname={} password={}".format(dbname, db_password))
        for fil in filter_lst:
            assert(isinstance(fil, filters.FilterBase))
        self.filters = filter_lst
        #compose all the fine pass filters into one function
        #TODO check that the order is preserved
        self.fine_pass = compose([fil.fine_pass for fil in filter_lst])
        self.group = compose([group.group for group in group_lst])

    def get_and_filter(self):
        '''
        Go to the DB and apply the filters
        '''
        cur = self.conn.cursor()
        sql_filters = sql.SQL(" AND ").join([fil.coarse_pass() for fil in self.filters])

        cur.execute(sql.SQL("SELECT * from journeys where {};").format(sql_filters))
        return self.fine_pass(cur)

    def combined(self):
        '''
        get the results from the db, apply the filters
        and group
        '''
        return self.group(self.get_and_filter())

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
