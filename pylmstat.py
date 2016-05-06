#!/usr/bin/env python

import argparse
from datetime import datetime, timedelta
import math
import operator
import os
import random
import re
import subprocess

try:
    # We only need matplotlib if plotting the hourly summary
    import matplotlib
    matplotlib.use('Agg')
    from matplotlib import pyplot as plt
except ImportError, RuntimeError:
    matplotlibExists = False
else:
    matplotlibExists = True

import numpy as np
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_utils import database_exists, create_database

# These are the products that will be parsed & stored
PRODUCT_LIST = [ 'MATLAB', 'SIMULINK', 'Image_Toolbox', 'Optimization_Toolbox', 'Signal_Toolbox', 'Statistics_Toolbox' ]
# The command to query the lmstat server
LMSTAT_COMMAND = [ "/usr/local/MATLAB/R2011b/etc/glnx86/lmutil", "lmstat", "-c", "/usr/local/MATLAB/R2011b/licenses/network.lic", "-a" ]

Base = declarative_base()

# The table classes
class Lmstats_MATLAB(Base):
    __tablename__ = 'Lmstats_MATLAB'
    datetime = Column(DateTime, primary_key=True)
    inuse = Column(Integer)
    users = Column(String)

class Lmstats_SIMULINK(Base):
    __tablename__ = 'Lmstats_SIMULINK'
    datetime = Column(DateTime, primary_key=True)
    inuse = Column(Integer)
    users = Column(String)

class Lmstats_Image_Toolbox(Base):
    __tablename__ = 'Lmstats_Image_Toolbox'
    datetime = Column(DateTime, primary_key=True)
    inuse = Column(Integer)
    users = Column(String)

class Lmstats_Optimization_Toolbox(Base):
    __tablename__ = 'Lmstats_Optimization_Toolbox'
    datetime = Column(DateTime, primary_key=True)
    inuse = Column(Integer)
    users = Column(String)

class Lmstats_Signal_Toolbox(Base):
    __tablename__ = 'Lmstats_Signal_Toolbox'
    datetime = Column(DateTime, primary_key=True)
    inuse = Column(Integer)
    users = Column(String)

class Lmstats_Statistics_Toolbox(Base):
    __tablename__ = 'Lmstats_Statistics_Toolbox'
    datetime = Column(DateTime, primary_key=True)
    inuse = Column(Integer)
    users = Column(String)


class Lmstat(object):
    """
    This general-purpose class can be used to query the lmstat server, insert the data
    into the database, analyse the data and export to TSV file. It can slao create a
    dummy database and list or plot the aggregated daily profile.
    """
    # The database is placed in the same directory as this script
    script_path = os.path.dirname(os.path.realpath(__file__))
    db_path = os.path.join(script_path, "lmstat.db")

    def __init__(self, db_url, verbose=False):
        self.verbose = verbose
        # self.engine = create_engine("sqlite:///{}".format(self.db_path))

        # self.engine = create_engine("sqlite:///lmstat.db")
#docker run --name lmstat -e POSTGRES_USER=usr -e POSTGRES_PASSWORD=pwd -d -p=5432:5432 postgres
        # self.engine = create_engine("postgresql://usr:pwd@localhost/lmstat")

        self.engine = create_engine(db_url)

        # self.engine.echo = True
        # self.metadata = MetaData(bind=self.engine)

        # if (not os.path.exists(self.db_path)):
        #     Base.metadata.create_all(self.engine)

        # Create database and tables
        if not database_exists(self.engine.url):
            create_database(self.engine.url)
            Base.metadata.create_all(self.engine)

        self.Tables = { }
        self.Usage = { }
        # Create the table objects and usage data structure
        for product in PRODUCT_LIST:
            if (product == 'MATLAB'):
                self.Tables[product] = Lmstats_MATLAB
            elif (product == 'SIMULINK'):
                self.Tables[product] = Lmstats_SIMULINK
            elif (product == 'Image_Toolbox'):
                self.Tables[product] = Lmstats_Image_Toolbox
            elif (product == 'Optimization_Toolbox'):
                self.Tables[product] = Lmstats_Optimization_Toolbox
            elif (product == 'Signal_Toolbox'):
                self.Tables[product] = Lmstats_Signal_Toolbox
            elif (product == 'Statistics_Toolbox'):
                self.Tables[product] = Lmstats_Statistics_Toolbox
            self.Usage[product] = { \
                'inuse_hour_max_max': [ 0 for _ in range(24) ],
                'inuse_hour_avg_avg': [ 0 for _ in range(24) ],
                'inuse_hour_avg_today': [ 0 for _ in range(24) ],
                'users_hour_today': [ 0 for _ in range(24) ],
                'datetime_day': [ ],
                'inuse_hour_date_avg': [ ],
                'users_hour_date': [ ],
                'date_day': [ ],
                'inuse_day_avg': [ ],
                'users_day': [ ],
                'users': { }
            }

    def query(self):
        """
        Query the lmstat server and return the output.
        """
        try:
            lmstat_out = subprocess.check_output(LMSTAT_COMMAND)
        except:
            lmstat_outs = [ ]
        else:
            lmstat_outs = lmstat_out.split('\n')

        if self.verbose:
            print lmstat_outs

        return lmstat_outs

    def read(self, input_path):
        """
        Read an output file and return the text.
        """
        with open(input_path, 'r') as f:
            lmstat_outs = f.readlines()

        return lmstat_outs

    def insert(self, lmstat_outs):
        """
        Parse the lmstat output and insert the data into the database.
        """
        inuse = None
        users = [ ]
        Session = sessionmaker(bind=self.engine)
        session = Session()
        # Scan through each line of output
        for lmstat_line in lmstat_outs:
            if (lmstat_line == ""):
                continue

            if (inuse is None):
# The product lines look like this:
# Users of MATLAB:  (Total of 35 licenses issued;  Total of 4 licenses in use)
# Users of Image_Toolbox:  (Total of 7 licenses issued;  Total of 1 license in use)
                prod_tokens = re.match(r'Users\s+of\s+([\w_]+):\s*\(Total\s+of\s+(\d+)\s+licenses?\s+issued;\s*Total\s+of\s+(\d+)\s+licenses?\s+in\s+use\)', lmstat_line, re.I)

                if (prod_tokens is not None):
                    # Record the product name and number of licenses currently in use
                    product = prod_tokens.group(1)
                    total = int(prod_tokens.group(2))
                    inuse = int(prod_tokens.group(3))
                    if self.verbose:
                        print product + " (" + prod_tokens.group(3) + "/" + prod_tokens.group(2) + ")"
                    if (inuse == 0):
                        inuse = None
                    continue

            if (inuse is not None):
# The user lines look like this:
# user-01 SERVER-NAME-1 PORT-NAME-1 (v20) (lmstat.host.com/1712 3333), start Tue 2/24 16:44
# user-02 SERVER-NAME-2 PORT-NAME-2 (v30) (lmstat.host.com/1712 2622), start Tue 2/24 16:24
# user-03 SERVER-NAME-3 PORT-NAME-3 (v30) (lmstat.com/1712 3634), start Tue 2/24 14:50
# user-04 SERVER-NAME-4 PORT-NAME-4 (v26) (lmstat.com/1712 1623), start Tue 2/24 16:26
# user-05 server-name-5 /dev/pts/6 (v32) (lmstat.com/1712 2391), start Wed 9/16 13:58
                user_tokens = re.match(r'\s+(\w+)\s+([\w\-]+)\s+([\w\-\/]+)\s+\(v(\d+)\)\s+\(([\w\.]+)\/(\d+)\s+(\d+)\),\s*start\s+([\w\/\s\:]+)', lmstat_line, re.I)

                if (user_tokens is not None):
                    # Append the name for each user to a list
                    user = user_tokens.group(1)
                    users.append(user)
                    if self.verbose:
                        print "  " + user

                elif (len(users) == inuse):
                    dt_now = datetime.now()
                    # Join the usernames into a comma-separated string
                    userstr = ','.join(users)

                    if (product in PRODUCT_LIST):
                        # Add the data to the appropriate database table
                        new_lmstat = self.Tables[product](datetime=dt_now, inuse=inuse, users=userstr)
                        session.add(new_lmstat)
                        session.commit()

                    inuse = None
                    users = [ ]

    def create(self, dayrange):
        """
        Create a mock database.
        """
        Session = sessionmaker(bind=self.engine)
        session = Session()

        # Clear the tables first
        for product in PRODUCT_LIST:
            session.query(self.Tables[product]).delete()
            session.commit()

        users = [ "user-%02d" % _ for _ in range(40) ]
        current_time = datetime.now()
        current_day = datetime(current_time.year, current_time.month, current_time.day)
        for dayspast in range(dayrange - 1, -1, -1):
            daystart = current_day - timedelta(days=dayspast)
            if (daystart.weekday() in [ 5, 6 ]):
                continue # No stats on weekend
            for product in PRODUCT_LIST:
                if (product == 'MATLAB'):
                    inuse_allday_max_max = ( 25.0, 10.0 ) # Maximum ~ 35 on Monday
                else:
                    inuse_allday_max_max = ( 3.0, 2.0 ) # Maximum ~ 5 on Monday

                inuse_allday, bin_edges = np.histogram(np.random.randn(1000), 24*4, density=True) # New data every 15mins
                inuse_allday_max = inuse_allday_max_max[0] + inuse_allday_max_max[1]/4.0*(4 - daystart.weekday())
                inuse_allday *= math.sqrt(2.0*math.pi)*inuse_allday_max
                hourmax = 24
                if (dayspast == 0): # Current day is incomplete
                    hourmax = 15
                for hour in range(hourmax):
                    dt_now = daystart + timedelta(hours=hour)
                    for qhour in range(4):
                        dt_now = current_day - timedelta(days=dayspast) + timedelta(hours=hour) + timedelta(minutes=qhour*15)
                        inuse = int(inuse_allday[hour*4 + qhour])
                        random.shuffle(users)
                        userstr = ','.join(users[:inuse])

                        new_lmstat = self.Tables[product](datetime=dt_now, inuse=inuse, users=userstr)

                        session.add(new_lmstat)
                        session.commit()

    def analyse(self):
        """
        Compute the hourly maximum and average users for each product over a number of days.
        """
        # create a configured "Session" class
        Session = sessionmaker(bind=self.engine)

        # create a Session
        session = Session()

        current_time = datetime.now()
        current_day = datetime(current_time.year, current_time.month, current_time.day)

        for hour in range(24):
            for product in PRODUCT_LIST:
                inuse_hour_avg_tot = 0
                for dayspast in range(100):
                    dayhourstart = current_day - timedelta(days=dayspast) + timedelta(hours=hour)
                    dayhourend = dayhourstart + timedelta(hours=1)
                    rs = session.query(self.Tables[product]).filter(dayhourstart <= self.Tables[product].datetime).filter(self.Tables[product].datetime < dayhourend).all()
                    inuse_day_hour_tot = 0
                    for row in rs:
                        inuse_day_hour_tot += row.inuse
                        if (self.Usage[product]['inuse_hour_max_max'][hour] < row.inuse):
                            self.Usage[product]['inuse_hour_max_max'][hour] = row.inuse
                        # print "%s: %s licenses in use (%s)" % (row.datetime, row.matlab_inuse, row.users)

                    if (dayspast == 0):
                        users_hour = [ ]
                        if (len(rs) > 0):
                            self.Usage[product]['inuse_hour_avg_today'][hour] = inuse_day_hour_tot/float(len(rs))
                        for row in rs:
                            userstr = row.users
                            if len(userstr):
                                users = userstr.split(',')
                                for user in users:
                                    if (user not in users_hour):
                                        users_hour.append(user)
                        self.Usage[product]['users_hour_today'][hour] = ', '.join(users_hour)

                    # Average users over hour
                    if (len(rs) > 0):
                        inuse_day_hour_avg = inuse_day_hour_tot/float(len(rs))
                        inuse_hour_avg_tot += inuse_day_hour_avg
                    # print dayhourstart, dayhourend, inuse_day_hour_tot

                # Average users over all days
                self.Usage[product]['inuse_hour_avg_avg'][hour] = inuse_hour_avg_tot/float(dayspast + 1)

    def analyse_days(self):
        """
        Compute the average users for each day and product over every hour.
        """
        # create a configured "Session" class
        Session = sessionmaker(bind=self.engine)

        # create a Session
        session = Session()

        current_time = datetime.now()
        current_day = datetime(current_time.year, current_time.month, current_time.day)

        for product in PRODUCT_LIST:
            self.Usage[product]['users'] = { }

        for dayspast in range(100):
            for product in PRODUCT_LIST:
                self.Usage[product]['datetime_day'].append([ "" for _ in range(24) ])
                self.Usage[product]['inuse_hour_date_avg'].append([ 0.0 for _ in range(24) ])
                self.Usage[product]['users_hour_date'].append([ "" for _ in range(24) ])
                for hour in range(24):
                    dayhourstart = current_day - timedelta(days=dayspast) + timedelta(hours=hour)
                    dayhourend = dayhourstart + timedelta(hours=1)
                    rs = session.query(self.Tables[product]).filter(dayhourstart <= self.Tables[product].datetime).filter(self.Tables[product].datetime < dayhourend).all()
                    inuse_day_hour_tot = 0
                    for row in rs:
                        inuse_day_hour_tot += row.inuse

                    self.Usage[product]['datetime_day'][dayspast][hour] = dayhourstart.strftime("%Y-%m-%d")

                    users_hour = [ ]
                    if (len(rs) > 0):
                        self.Usage[product]['inuse_hour_date_avg'][dayspast][hour] = inuse_day_hour_tot/float(len(rs))
                    for row in rs:
                        userstr = row.users
                        if len(userstr):
                            users = userstr.split(',')
                            for user in users:
                                if (user not in users_hour):
                                    users_hour.append(user)

                            for user in users_hour:
                                if (user not in self.Usage[product]['users']):
                                    self.Usage[product]['users'][user] = 1
                                else:
                                    self.Usage[product]['users'][user] += 1

                    self.Usage[product]['users_hour_date'][dayspast][hour] = ', '.join(users_hour)

    def analyse_year(self):
        """
        Compute the average users for each day and product.
        """
        # create a configured "Session" class
        Session = sessionmaker(bind=self.engine)

        # create a Session
        session = Session()

        current_time = datetime.now()
        current_day = datetime(current_time.year, current_time.month, current_time.day)

        for product in PRODUCT_LIST:
            self.Usage[product]['users_day'] = [ ]

        for dayspast in range(100):
            for product in PRODUCT_LIST:
                daystart = current_day - timedelta(days=dayspast)
                dayend = daystart + timedelta(days=1)
                rs = session.query(self.Tables[product]).filter(daystart <= self.Tables[product].datetime).filter(self.Tables[product].datetime < dayend).all()
                inuse_day_tot = 0
                users_day = [ ]
                for row in rs:
                    inuse_day_tot += row.inuse
                    userstr = row.users
                    if len(userstr):
                        users = userstr.split(',')
                        for user in users:
                            if (user not in users_day):
                                users_day.append(user)

                inuse_day_avg = 0.0
                # Average users over day
                if (len(rs) > 0):
                    inuse_day_avg = inuse_day_tot/float(len(rs))

                    self.Usage[product]['date_day'].append(daystart.strftime("%Y-%m-%d"))
                    self.Usage[product]['inuse_day_avg'].append(inuse_day_avg)
                    self.Usage[product]['users_day'].append(', '.join(users_day))

    def list(self, product):
        # for product in PRODUCT_LIST:
        print "%s" % product
        for hour in range(24):
            # print "%2d: %s (%d)" % (hour, '*' * int(self.Usage[product]['inuse_hour_avg_avg'][hour]), int(self.Usage[product]['inuse_hour_avg_avg'][hour]))
            print "%2d: %s (%d)" % (hour, '*' * int(self.Usage[product]['inuse_hour_max_max'][hour]), int(self.Usage[product]['inuse_hour_max_max'][hour]))

    def plot(self):
        plt.style.use('ggplot')
        # figsize = layout.figaspect(scale=1.2)
        # fig, ax = plt.subplots(figsize=figsize)
        i = 0
        for product in PRODUCT_LIST:
            fig, ax = plt.subplots(math.ceil(len(PRODUCT_LIST)/2.0), 2, i)

            b0 = ax.bar(np.arange(24), self.Usage[product]['inuse_hour_max_max'], color=plt.rcParams['axes.color_cycle'][2], width=0.30)
            b1 = ax.bar(np.arange(24) + 0.30, self.Usage[product]['inuse_hour_avg_avg'], color=plt.rcParams['axes.color_cycle'][1], width=0.30)
            b2 = ax.bar(np.arange(24) + 0.60, self.Usage[product]['inuse_hour_avg_today'], color=plt.rcParams['axes.color_cycle'][0], width=0.30)
            ax.legend(( b0[0], b1[0], b2[0] ), ( "Max", "Mean", "Today" ))
            ax.set_xlabel("time (hours)")
            ax.set_ylabel("licenses (int)")
            ax.set_title("%s licenses in use" % product)
            i += 1
        plt.show()

    def export(self, export_path):
        """
        Export the aggregate tables to file.
        """
        # export_path = os.path.join(os.environ['HOME'], "public_html", "Lmstat")
        for product in PRODUCT_LIST:
            with open(os.path.join(export_path, "lmstat-%s.tsv" % product), 'w') as f:
                f.write("%s\t%s\t%s\t%s\t%s\n" % ("Hour", "Maximum", "Average", "Daily", "Names"))
                for hour in range(24):
                    f.write("%d\t%f\t%f\t%f\t%s\n" % (hour, self.Usage[product]['inuse_hour_max_max'][hour], self.Usage[product]['inuse_hour_avg_avg'][hour], self.Usage[product]['inuse_hour_avg_today'][hour], self.Usage[product]['users_hour_today'][hour]))
            with open(os.path.join(export_path, "lmstat-%s-days.tsv" % product), 'w') as f:
                f.write("%s\t%s\t%s\t%s\n" % ("Date", "Hour", "Daily", "Names"))
                for day in range(len(self.Usage[product]['inuse_hour_date_avg'])):
                    for hour in range(len(self.Usage[product]['inuse_hour_date_avg'][day])):
                        f.write("%s\t%02d\t%f\t%s\n" % (self.Usage[product]['datetime_day'][day][hour], hour, self.Usage[product]['inuse_hour_date_avg'][day][hour], self.Usage[product]['users_hour_date'][day][hour]))
            with open(os.path.join(export_path, "lmstat-%s-year.tsv" % product), 'w') as f:
                f.write("%s\t%s\t%s\n" % ("Date", "Average", "Names"))
                for day in range(len(self.Usage[product]['inuse_day_avg'])):
                    if (self.Usage[product]['inuse_day_avg'][day] > 0):
                        f.write("%s\t%f\t%s\n" % (self.Usage[product]['date_day'][day], self.Usage[product]['inuse_day_avg'][day], self.Usage[product]['users_day'][day]))

            users = sorted(self.Usage[product]['users'].items(), key=operator.itemgetter(1), reverse=True)
            with open(os.path.join(export_path, "lmstat-%s-users.tsv" % product), 'w') as f:
                f.write("%s\t%s\n" % ("User", "Hours"))
                for (user, hours) in users:
                    f.write("%s\t%d\n" % (user, hours))

def main():

    parser = argparse.ArgumentParser(description="Query and collate stats from Matlab license server.")
    # parser.add_argument('-i', help="Initialise SQLite database")
    # parser.add_argument('-c', help="Create a mock database", action='store_true')
    parser.add_argument('-d', help="Database URL ['sqlite:///lmstat.db']", nargs='?', const='sqlite:///lmstat.db')
    parser.add_argument('-c', help="Create a mock database of N days", default=0, type=int)
    parser.add_argument('-q', help="Query the lmstat server", action='store_true')
    parser.add_argument('-r', help="Lmstat output file to read ['lmstat.txt']", nargs='?', const='lmstat.txt')
    parser.add_argument('-i', help="Insert a new lmstat query result into the db", action='store_true')
    parser.add_argument('-l', help="List an hourly summary of the product ['MATLAB']", nargs='?', const='MATLAB')
    parser.add_argument('-p', help="Plot an hourly summary of the data", action='store_true')
    parser.add_argument('-e', help="Export directory for the summary data files ['.']", nargs='?', const='.')
    parser.add_argument('-v', help="Verbose output", action='store_true')

    args = parser.parse_args()

    lmstat = Lmstat(args.d, verbose=args.v)
    if (args.c > 0):
        lmstat.create(args.c)
    else:
        lmstat_outs = [ ]
        if args.q:
            lmstat_outs = lmstat.query()
        elif args.r:
            lmstat_outs = lmstat.read(args.r)

        if args.i:
            lmstat.insert(lmstat_outs)

    if (args.l or args.p or args.e):
        lmstat.analyse()
        lmstat.analyse_days()
        lmstat.analyse_year()
    if args.l:
        lmstat.list(args.l)
    elif args.p:
        if matplotlibExists:
            lmstat.plot()
        else:
            sys.stderr.write("Error: The Matplotlib package must exist in order to use the plotting function.\n")
    elif args.e:
        lmstat.export(args.e)

if __name__ == '__main__':

    main()
