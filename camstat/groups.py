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

class GroupByVisitedSites(GroupBase):
    '''
    Group the rows by any site visited
    (this will duplicate rows as each journey may visit multiple sites)

    if start_site is specified only put a journey into a site group
    if that site was visited after visiting the start_site
    '''

    def __init__(self, start_site=None):
        self.start_site = start_site

    def group_rows(self, rows):
        groups = collections.defaultdict(list)
        for row in rows:
            journey_chain = row[CHAIN_COLUMN_INDEX].split(">")
            if self.start_site:
                #only search starting from the first instnce of start_site
                start_index = journey_chain.index(self.start_site)
                journey_chain = journey_chain[start_index:]
            for site in set(journey_chain):
                groups[site].append(row)
        return groups
