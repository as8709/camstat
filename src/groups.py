import abc
import collections
TIMESTAMP_COLUMN_INDEX = 1
CLASS_COLUMN_INDEX = 2
TOTAL_TIME_COLUMN_INDEX = 3
CHAIN_COLUMN_INDEX = 4
CHAIN_TIME_COLUMN_INDEX = 5

class GroupBase(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def group_rows(self, rows):
        return

    def group(self, groups):
        if isinstance(groups, list):
            return self.group_rows(groups)
        elif isinstance(groups, dict):
            return {key:self.group(value) for key, value in groups.items()}
        raise Exception("Unknown group type:{}".format(type(groups)))


class GroupByHour(GroupBase):

    def group_rows(self, rows):
        groups = collections.defaultdict(list)
        for row in rows:
            start_time = row[TIMESTAMP_COLUMN_INDEX]
            groups[start_time.hour].append(row)
        return groups

class GroupByClass(GroupBase):
    def group_rows(self, rows):
        groups = collections.defaultdict(list)
        for row in rows:
            vehicle_class = row[CLASS_COLUMN_INDEX]
            groups[vehicle_class].append(row)
        return groups

class GroupByStartSite(GroupBase):
    def group_rows(self, rows):
        groups = collections.defaultdict(list)
        for row in rows:
            journey_chain = row[CHAIN_COLUMN_INDEX]
            start_site = journey_chain.split(">")[0]
            groups[start_site].append(row)
        return groups
