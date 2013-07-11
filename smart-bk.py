#!/usr/bin/env python
# Andrew Oatley-Willis
# Smart backup script
# Should be used to make intelligent backups of systems so systems are not overloaded
# Should be made to be simple to use and configure
import datetime
import optparse
import pysftp
import sys
import urllib2
import getpass
import crypt
import random
import re
import string
import subprocess
import os
import sqlite3 as lite

# Run a backup command?
class backup:
    def __init__(self):
        self.timestarted = time.ctime(time.time())

    def __str__(self):
        return self.timestarted

# Create/remove a schedule, get schedule information
class schedule:
    def __init__(self):
        self.schedule = self.listSchedule()
    
    # When you print object
    def __str__(self):
        return self.prettySchedule()
    
    # Create a new schedule
    def newSchedule(self, time, backuptype, sourcehost, desthost, sourcedir, destdir):
        try:
            con = lite.connect('schedule.db')
            cur = con.cursor()
            cur.execute('INSERT INTO Schedule(time, type, source_host, dest_host, source_dir, dest_dir) VALUES(?, ?, ?, ?, ?, ?)', (time, backuptype, sourcehost, desthost, sourcedir, destdir))
            con.commit()
            con.close()
        except lite.Error, e:
            if con:
                con.rollback()
                con.close()
            print 'Error: ' + e.args[0]
            exit()
    
    # Give a schedule id and delete that schedule
    def removeSchedule(self, scheduleid):
        try:
            con = lite.connect('schedule.db')
            cur = con.cursor()
            cur.execute('DELETE FROM Schedule WHERE id = ?', (scheduleid))
            con.commit()
            con.close()
        except lite.Error, e:
            if con:
                con.rollback()
                con.close()
            print 'Error: ' + e.args[0]
            exit()
    
    # Output the schedule in a list
    def listSchedule(self):
        schedule = []
        try:
            con = lite.connect('schedule.db')
            with con:
                cur = con.cursor()
                case = cur.execute('SELECT * FROM Schedule')
                rows = cur.fetchall()
                for row in rows:
                    schedule.append([row[0], row[1].strip(), row[2].strip(), row[3].strip(), row[4].strip(), row[5].strip(), row[6].strip()])
                return schedule
        except lite.Error, e:
            if con:
                con.rollback()
                con.close()
            print 'Error: ' + e.args[0]
            exit()
    
    # Make the schedule look pretty and output it
    def prettySchedule(self):
        for item in self.schedule:
            print "|\t" + str(item[0]) + "\t|\t" + item[1] + "\t\t|\t" + item[2] + "\t\t|\t" + item[3] + "\t\t|\t" + item[4] + "\t\t|\t" + item[5] + "\t\t|\t" + item[6] + "\t|"
        return ""

    # Check current time and time on schedules, add to queue if time passed
    def queueSchedule(self):
        now = str(datetime.datetime.now()).split(' ')
        hours, minutes, seconds = now[1].split(':')
        time = (hours * 60 * 60) + (minutes * 60)
        for item in self.schedule:
            shours, sminutes = item[1].split(':')
            schedtime = (shours * 60 * 60) + (sminutes * 60)
            if time >= schedtime:
                # Add to queue
                try:
                    con = lite.connect('schedule.db')
                    cur = con.cursor()
                    cur.execute('INSERT INTO Schedule(time, type, source_host, dest_host, source_dir, dest_dir) VALUES(?, ?, ?, ?, ?, ?)', (time, backuptype, sourcehost, desthost, sourcedir, destdir))
                    con.commit()
                    con.close()
                except lite.Error, e:
                    if con:
                        con.rollback()
                        con.close()
                    print 'Error: ' + e.args[0]
                    exit()








def main():
    # Create command line options
    parser = optparse.OptionParser()
    parser = optparse.OptionParser(usage='Usage: %prog [options]')
    parser.add_option('-a', '--add',    help='Add new schedule at specific time', dest='add', default=False, action='store_true')
    parser.add_option('-r', '--remove',    help='Remove existing schedule', dest='remove', default=False, action='store_true')
    parser.add_option('-s', '--show',    help='Show the schedule and host info', dest='show', default=False, action='store_true')
    parser.add_option('-q', '--queue',    help='Add schedules to queue', dest='queue', default=False, action='store_true')
    parser.add_option('--time',    help='specify the time to run the backup', dest='time', default=False, action='store', metavar="18:00")
    parser.add_option('--backup-type',    help='rsync, snapshot, dbdump', dest='backuptype', default=False, action='store', metavar="type")
    parser.add_option('--source-host',    help='specify the source backup host', dest='sourcehost', default=False, action='store', metavar="host")
    parser.add_option('--dest-host',    help='specify the destination backup host', dest='desthost', default=False, action='store', metavar="host")
    parser.add_option('--source-dir',    help='specify the source backup dir', dest='sourcedir', default=False, action='store', metavar="dir")
    parser.add_option('--dest-dir',    help='specify the destination backup dir', dest='destdir', default=False, action='store', metavar="dir")
    parser.add_option('--sid',    help='specify schedule id for removing schedules', dest='sid', default=False, action='store', metavar="schedule id")
    (opts, args) = parser.parse_args()
    
    # No options entered
    if len(sys.argv[1:]) == 0:
        parser.print_help()
        exit(-1)

    # Option switches
    if opts.time:
        time = opts.time
    if opts.sid:
        scheduleid = opts.sid
    if opts.backuptype:
        backuptype = opts.backuptype
    if opts.sourcehost:
        sourcehost = opts.sourcehost
    if opts.desthost:
        desthost = opts.desthost
    if opts.sourcedir:
        sourcedir = opts.sourcedir
    if opts.destdir:
        destdir = opts.destdir

    # Option dependencies
    if opts.remove and not opts.sid:
        print "Option remove requires option sid"
        parser.print_help()
        exit(-1)
    if opts.add:
        if not opts.time or not opts.backuptype or not opts.sourcehost or not opts.desthost or not opts.sourcedir or not opts.destdir:
            print "Option add requires option time, backup-type, source-host, dest-host, source-dir, dest-dir"
            parser.print_help()
            exit(-1)

    # Weird use cases
    if opts.add and opts.remove:
        parser.print_help()
        exit(-1)

    # Start program
    scheduler = schedule()
    if opts.show:
        scheduler = schedule()
        print scheduler
    elif opts.add:
        print time, backuptype, sourcehost, desthost, sourcedir, destdir
        scheduler.newSchedule(time, backuptype, sourcehost, desthost, sourcedir, destdir)
    elif opts.remove:
        scheduler.removeSchedule(scheduleid)




if __name__ == '__main__':
    main()
