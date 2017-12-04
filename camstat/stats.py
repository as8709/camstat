import abc
import datetime

TIMESTAMP_COLUMN_INDEX = 1
CLASS_COLUMN_INDEX = 2
TOTAL_TIME_COLUMN_INDEX = 3
CHAIN_COLUMN_INDEX = 4
CHAIN_TIME_COLUMN_INDEX = 5

class BaseStats(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def make_stats(self, rows):
        return

    @abc.abstractmethod
    def stat_descriptions(self):
        return


class TimeStats(BaseStats):
    '''

    '''
    def make_stats(self, rows):
        trip_times = [row[TOTAL_TIME_COLUMN_INDEX] for row in rows]
        if len(trip_times): #avoid divide by zero error
            average_trip_time = sum(trip_times, datetime.timedelta())/len(trip_times)
        else:
            average_trip_time = None
        return [min(trip_times).seconds, max(trip_times).seconds, average_trip_time.seconds]


    def stat_descriptions(self):
        return ["Min trip time(s)", "Max trip time(s)", "Avg. trip time"]

class NStats(BaseStats):
    def make_stats(self, rows):
        n_journeys = len(rows)
        veh_classes = [row[CLASS_COLUMN_INDEX] for row in rows]
        class_summary = {}
        for veh_class in set(veh_classes):
            n_class = len([c for c in veh_classes if veh_class == c])
            class_summary[veh_class] = (n_class / n_journeys * 100, n_class)
        return [n_journeys, class_summary]

    def stat_descriptions(self):
        return ["No. journeys", "Journeys by class (percentage, No. journeys)"]
