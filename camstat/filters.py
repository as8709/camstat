import psycopg2 as psy
from psycopg2 import sql

import abc
import re
import datetime

TIMESTAMP_COLUMN_INDEX = 1
CLASS_COLUMN_INDEX = 2
TOTAL_TIME_COLUMN_INDEX = 3
CHAIN_COLUMN_INDEX = 4
CHAIN_TIME_COLUMN_INDEX = 5

class FilterBase(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def coarse_pass(self):
        '''
        Get the SQL where clause to filter from the db
        This will be concatenated with ANDs in the top level
        SELECT statement
        '''
        return

    @abc.abstractmethod
    def fine_pass(self, rows):
        '''
        Given some rows from the db return a filtered
        (and possibly altered) list of rows
        '''
        return

class SiteFilter(FilterBase):

    def __init__(self, route_regex):
        self.route_regex = route_regex
        self.sites = self.sites_from_regex(route_regex)

    def sites_from_regex(self, route_regex):
        '''
        Given a route regex extract any complete site names
        '''
        #TODO check the db to ensure that these sites exist as tables
        return [groups[0] for groups in re.findall(r"(\d\d\D?_([NESW]|(OUT)|(IN)))", route_regex)]

    def coarse_pass(self):
        '''
        At a minimum matching journeys must contain all the sites in the desired journey
        N.B. this won't check that the sites are visited in the right order that what fine_pass()
        is for
        '''
        #note for this class no site can be None, but it is useful to support
        #for derived classes
        site_queries = [sql.SQL("SELECT * from {}").format(sql.Identifier("s"+site)) for site in self.sites if site is not None]
        return sql.SQL("(journey_id in ({}))").format(sql.SQL(" INTERSECT ").join(site_queries))

    def fine_pass(self, rows):
        '''
        Iterate over the rows
        discard any rows which don't match the route_regex
        for any row that does extract the sub route(s) that match
        and change the start and end times to correspond to just the sub-route
        '''
        row_lists = [self.extract_route(row, self.route_regex) for row in rows if re.search(self.route_regex, row[CHAIN_COLUMN_INDEX])]
        #there could be multiple matches (and therefore output rows) per journey so extract_route
        # returns a list of lists, flatten this list
        return [x for rows in row_lists for x in rows]

    def extract_route(self, row, route_regex):
        '''
        Assuming this row contains at least one matching sub match_route
        Return the equivalent row for just that subroute, changing the times accordingly
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

class StartEndViaFilter(SiteFilter):
    def __init__(self, start, end, via, indirect_allowed):
        self.route_regex = self.make_route_regex(start, end, via, indirect_allowed)
        self.sites = [start, end] + via

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

class ClassFilter(FilterBase):
    def __init__(self, allowed_classes):
        self.allowed_classes = allowed_classes

    def coarse_pass(self):
        '''
        '''
        return sql.SQL("(class in {})").format(sql.Literal(tuple(self.allowed_classes)))

    def fine_pass(self, rows):
        '''
        SQL does all the filtering we need
        pass though unchanged
        '''
        return rows
