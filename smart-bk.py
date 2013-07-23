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

# Create/remove a schedule, get schedule information
class schedule:
    def __init__(self):
        self.backupuser = 'backup'
        self.updateTime()
        self.updateSchedules()

    # Update time
    def updateTime(self):
        # Get the date and time and store it
        self.now = str(datetime.datetime.now()).split(' ')
        self.year, self.month, self.day = self.now[0].split('-')
        self.hours, self.minutes, self.seconds = self.now[1].split(':')
        self.hours, self.minutes = int(self.hours), int(self.minutes)

    # Update schedules
    def updateSchedules(self):
        # Get database and put it in lists
        self.schedule, self.queue, self.running = self.listSchedule()
        # Get busy hosts and ids and schedules
        self.busyhosts = []
        self.busyids = []
        self.busyschedules = []
        for item in self.schedule:
            for run in self.running:
                if item[0] == run[0]:
                    if item not in self.busyschedules:
                        self.busyschedules.append(item)
                    if item[0] not in self.busyids:
                        self.busyids.append(item[0])
                    if item[4] not in self.busyhosts:
                        self.busyhosts.append(item[4])
                    if item[5] not in self.busyhosts:
                        self.busyhosts.append(item[5])
        # Get free hosts and ids and schedules
        self.freehosts = []
        self.freeids = []
        self.freeschedules = []
        for item in self.schedule:
            if item not in self.busyschedules and item not in self.freeschedules:
                self.freeschedules.append(item)
            if item[0] not in self.busyids and item[0] not in self.freeids:
                self.freeids.append(item[0])
            if item[4] not in self.busyhosts:
                self.freehosts.append(item[4])
            if item[5] not in self.busyhosts:
                self.freehosts.append(item[5])
        # Get queue hosts and ids and schedules
        self.queuehosts = []
        self.queueids = []
        self.queueschedules = []
        for item in self.schedule:
            for queue in self.queue:
                if item[0] == queue[0]:
                    if item not in self.queueschedules:
                        self.queueschedules.append(item)
                    if item[0] not in self.queueids:
                        self.queueids.append(item[0])
                    if item[4] not in self.queuehosts:
                        self.queuehosts.append(item[4])
                    if item[5] not in self.queuehosts:
                        self.queuehosts.append(item[5])
        
    
    # When you print object
    def __str__(self):
        return self.prettySchedule()
    
    # Create a new schedule
    def newSchedule(self, time, backuptype, sourcehost, desthost, sourcedir, destdir):
        output = self.day, time, backuptype, sourcehost, desthost, sourcedir, destdir
        self.writeLog(output)
        try:
            con = lite.connect('schedule.db')
            cur = con.cursor()
            cur.execute('INSERT INTO Schedule(day, time, type, source_host, dest_host, source_dir, dest_dir) VALUES(?, ?, ?, ?, ?, ?, ?)', (self.day, time, backuptype, sourcehost, desthost, sourcedir, destdir))
            con.commit()
            con.close()
        except lite.Error, e:
            if con:
                con.rollback()
                con.close()
            output = 'Error: ' + e.args[0]
            self.writeLog(output)
            exit()
    
    # Give a schedule id and delete that schedule
    def removeSchedule(self, scheduleid):
        output = 'Removing scheduleid = ' + str(scheduleid) + ' from all tables'
        self.writeLog(output)
        try:
            con = lite.connect('schedule.db')
            cur = con.cursor()
            cur.execute('DELETE FROM Queue WHERE scheduleid = ?', (scheduleid))
            cur.execute('DELETE FROM Running WHERE scheduleid = ?', (scheduleid))
            cur.execute('DELETE FROM Schedule WHERE id = ?', (scheduleid))
            con.commit()
            con.close()
        except lite.Error, e:
            if con:
                con.rollback()
                con.close()
            output = 'Error: ' + e.args[0]
            self.writeLog(output)
            exit()
    
    # Output the schedule in a list
    def listSchedule(self):
        schedule = []
        queue = []
        running = []
        try:
            con = lite.connect('schedule.db')
            with con:
                cur = con.cursor()
                case = cur.execute('SELECT * FROM Schedule')
                rows = cur.fetchall()
                for row in rows:
                    # id, day, time, type, sourcehost, desthost, sourcedir, destdir
                    schedule.append([row[0], row[1].strip(), row[2].strip(), row[3].strip(), row[4].strip(), row[5].strip(), row[6].strip(), row[7].strip()])
                case = cur.execute('SELECT * FROM Queue')
                rows = cur.fetchall()
                for row in rows:
                    # scheduleid, queuetime
                    queue.append([row[0], row[1].strip()])
                case = cur.execute('SELECT * FROM Running')
                rows = cur.fetchall()
                for row in rows:
                    # scheduleid, starttime
                    running.append([row[0], row[1].strip()])
                return schedule, queue, running
        except lite.Error, e:
            if con:
                con.rollback()
                con.close()
            output = 'Error: ' + e.args[0]
            self.writeLog(output)
            exit()
    
    # Make the schedule look pretty and output it
    def prettySchedule(self):
        # Schedule
        print "\n\t" * 10 + "-[Schedule]-"
        print "-" * 100
        print "id" + "|" + "day" + "|" + "time" + "|" + "type" + "|" + "source host" + "|" + "dest host" + "|" + "source dir" + "|" + "dest dir" 
        print "-" * 100
        for item in self.schedule:
            print str(item[0]) + "|" + item[1] + "|" + item[2] + "|" + item[3] + "|" + item[4] + "|" + item[5] + "|" + item[6] + "|" + item[7] 
        print "-" * 100
        # Queue
        print "\n" * 10 + "-[Queue]-"
        print "-" * 100
        print "|" + "schedule id" + "|" + "queue time" + "|"
        print "-" * 100
        for item in self.queue:
            print "|" + str(item[0]) + "|" + item[1] + "|"
        print "-" * 100
        # Running
        print "\n" * 10 + "-[Running]-"
        print "-" * 100
        print "|" + "schedule id" + "|" + "start time" + "|"
        print "-" * 100
        for item in self.running:
            print "|" + str(item[0]) + "|" + item[1] + "|"
        print "-" * 100
        return ""

    # Check current time and time on schedules, add to queue if time passed
    def queueSchedules(self):
        time = (self.hours * 60 * 60) + (self.minutes * 60)
        output = "Checking all schedules for expired times:"
        self.writeLog(output)
        for item in self.schedule:
            self.updateSchedules()
            shours, sminutes = item[2].split(':')
            shours, sminutes = int(shours), int(sminutes)
            schedtime = (shours * 60 * 60) + (sminutes * 60)
            lastday = item[1]
            scheduleid = item[0]
            if lastday == self.day:
                return False
            if scheduleid in self.queueids:
                continue 
            # If the scheduled time has passed, move schedule into queue
            if time >= schedtime:
                output = 'Adding scheduleid = ' + str(scheduleid) + ' to queue'
                self.writeLog(output)
                try:
                    # Add 0 for strings 1, 2, 3 to 01, 02, 03 - Important for minutes 12:03 looks like 12:3
                    if len(str(self.minutes)) == 1:
                        self.minutes = '0' + self.minutes
                    con = lite.connect('schedule.db')
                    cur = con.cursor()
                    cur.execute('INSERT INTO Queue(scheduleid, queuetime) VALUES(?, ?);', (scheduleid, str(self.hours) + ':' + str(self.minutes)))
                    cur.execute('UPDATE schedule SET day=? where id=?;', (str(self.day), scheduleid))
                    con.commit()
                except lite.Error, e:
                    if con:
                        con.rollback()
                    output = 'Error: ' + e.args[0]
                    self.writeLog(output)
                    exit()
                finally:
                    if con:
                        con.close()
        return True

    def queueSchedule(self, scheduleid):
        output = 'Adding scheduleid = ' + scheduleid + ' to queue'
        self.writeLog(output)
        if scheduleid in self.queueids:
            return False
        try:
            con = lite.connect('schedule.db')
            cur = con.cursor()
            cur.execute('INSERT INTO Queue(scheduleid, queuetime) VALUES(?, ?);', (scheduleid, str(self.hours) + ':' + str(self.minutes)))
            con.commit()
        except lite.Error, e:
            if con:
                con.rollback()
                output = 'Error: ' + e.args[0]
                self.writeLog(output)
                exit()
        finally:
            if con:
                con.close()
        return True
    
    def expireSchedule(self, scheduleid):
        output = 'Marking scheduleid = ' + scheduleid + ' as expired'
        self.writeLog(output)
        try:
            con = lite.connect('schedule.db')
            cur = con.cursor()
            cur.execute('UPDATE Schedule SET day=? where id = ?;', (str(int(self.day)-1), scheduleid))
            con.commit()
        except lite.Error, e:
            if con:
                con.rollback()
                output = 'Error: ' + e.args[0]
                self.writeLog(output)
                exit()
        finally:
            if con:
                con.close()
        return True


    # Delete a single queue
    def removeQueue(self, scheduleid):
        output = 'Deleting scheduleid = ' + scheduleid + ' from queue'
        self.writeLog(output)
        try:
            con = lite.connect('schedule.db')
            cur = con.cursor()
            cur.execute('DELETE FROM Queue WHERE scheduleid = ?', (scheduleid))
            con.commit()
        except lite.Error, e:
            if con:
                con.rollback()
            output = 'Error: ' + e.args[0]
            self.writeLog(output)
            exit()
        finally:
            if con:
                con.close()
    
    # Delete a single run
    def removeRunning(self, scheduleid):
        output = 'Deleting scheduleid = ' + scheduleid + ' from running'
        self.writeLog(output)
        try:
            con = lite.connect('schedule.db')
            cur = con.cursor()
            cur.execute('DELETE FROM Running WHERE scheduleid = ?', (scheduleid))
            con.commit()
        except lite.Error, e:
            if con:
                con.rollback()
            output = 'Error: ' + e.args[0]
            self.writeLog(output)
            exit()
        finally:
            if con:
                con.close()

    # Add a new running instance and make sure one isn't already running
    def newRunning(self, scheduleid):
        output = 'Adding scheduleid = ' + scheduleid + ' to running'
        self.writeLog(output)
        if scheduleid in self.queueids:
            return False
        try:
            con = lite.connect('schedule.db')
            cur = con.cursor()
            cur.execute('INSERT INTO running(scheduleid, starttime) VALUES(?, ?);', (scheduleid, str(self.hours) + ':' + str(self.minutes)))
            con.commit()
        except lite.Error, e:
            if con:
                con.rollback()
                output = 'Error: ' + e.args[0]
                self.writeLog(output)
                exit()
        finally:
            if con:
                con.close()
        return True


    # Find all hosts in queue, find which one needs to be run first, move hosts to running if no conflicts
    def startBackup(self):
        hosts = []
        if not self.queueschedules:
                return False
        for row in self.queueschedules:
            self.updateSchedules()
            # Easy to use variables for backups
            self.scheduleid = str(row[0])
            self.backuptype = row[3]
            self.sourcehost = row[4]
            self.desthost = row[5]
            self.sourcedir = row[6]
            self.destdir = row[7]
            # Check if this backup is already running
            if row in self.busyschedules:
                output = 'Busy scheduleid = ' + self.scheduleid + ' already running'
                self.writeLog(output)
                continue
            if self.sourcehost in self.busyhosts or self.desthost in self.busyhosts:
                output = 'Busy scheduleid = ' + self.scheduleid + ' busy hosts = ', self.busyhosts
                self.writeLog(output)
                continue
            # Check hosts for connectivity
            if not connectHost(self.sourcehost):
                output = 'Unavailable host = ' + self.sourcehost + ' no connection'
                self.writeLog(output)
                continue
            if not connectHost(self.desthost):
                output = 'Unavailable host = ' + self.desthost + ' no connection'
                self.writeLog(output)
                continue
            hosts.append(self.hostsource)
            hosts.append(self.hostdest)
            self.removeQueue(self.scheduleid)
            self.newRunning(self.scheduleid)
            # Start the backup here 
            # Finish backup here
            self.removeRunning(self.scheduleid)
        return True 
    
    # Check all hosts in the schedule for connection issues
    
    # Log everything
    def writeLog(self, output):
        print str(output)
        log = ""
        if len(str(self.minutes)) == 1:
            self.minutes = '0' + str(self.minutes)
        try:
            log = open('/home/' + self.backupuser + '/logs/smartbk-' + str(self.year) + '-' + str(self.month) + '-' + str(self.day) + '-' + str(self.hours) + '-' + str(self.minutes) + '.log', 'a+')
            log.write(str(output)+'\n')
        except Exception, e:
            print "Error: " + str(e)
            pass
        finally:
            if log:
                log.close()
             

    # Connect to the hosts, return True if success or False if not successful
    def connectHost(self, host):
        try:
            response=urllib2.urlopen('http://'+host,timeout=1)
            srv = pysftp.Connection(host=host, username=self.backupuser, log=True)
            srv.close()
            return True
        except urllib2.URLError as err:pass
        except:pass
        return False
                                                                                                

    # Backup is complete, clean, log, and email results
    def performRsync(self):
        srv = pysftp.Connection(host=self.sourcehost, username="backup", log=True)
        output = srv.execute('rsync -aHAXEvz --exclude "lost+found" ' + self.backupuser + '@' + self.sourcehost + ':' + self.sourcedir + self.backupuser + '@' + self.desthost + ':' + self.destdir)
        self.writeLog(output)
        srv.close()

    def performDbdump(self):
        print "Start dbdump"

    def performSnapshot(self):
        print "Start snapshot"

def main():
    # Create command line options
    desc = """The program %prog is used to run backups from computer to computer. %prog does this by adding and removing schedules
from a schedule database. Once added to the schedule database, %prog should be run with '--queue' in order to intelligently
add hosts to a queue and start running backups. It is recommended to run this as a cron job fairly often, more fequently
depending on the number of schedules."""
    parser = optparse.OptionParser(description=desc, usage='Usage: %prog [options]')
    parser.add_option('-a', '--add',    help='add new schedule at specific time', dest='add', default=False, action='store_true')
    parser.add_option('-s', '--show',    help='show the schedule and host info', dest='show', default=False, action='store_true')
    parser.add_option('-q', '--queue',    help='search and add expired schedules to queue', dest='queue', default=False, action='store_true')
    parser.add_option('-r', '--remove',    help='remove existing schedule', dest='remove', default=False, action='store_true')
    parser.add_option('--remove-queue',    help='remove existing schedule from queue', dest='removequeue', default=False, action='store_true')
    parser.add_option('--remove-run',    help='remove existing schedule from running', dest='removerun', default=False, action='store_true')
    parser.add_option('--expire',    help='expire the day in schedule', dest='expire', default=False, action='store_true')
    parser.add_option('--add-queue',    help='add a single schedule to queue', dest='addqueue', default=False, action='store_true')
    parser.add_option('--sid',    help='specify schedule id for removing schedules', dest='sid', default=False, action='store', metavar="scheduleid")
    parser.add_option('--time',    help='specify the time to run the backup', dest='time', default=False, action='store', metavar="18:00")
    parser.add_option('--backup-type',    help='rsync, snapshot, dbdump', dest='backuptype', default=False, action='store', metavar="type")
    parser.add_option('--source-host',    help='specify the source backup host', dest='sourcehost', default=False, action='store', metavar="host")
    parser.add_option('--dest-host',    help='specify the destination backup host', dest='desthost', default=False, action='store', metavar="host")
    parser.add_option('--source-dir',    help='specify the source backup dir', dest='sourcedir', default=False, action='store', metavar="dir")
    parser.add_option('--dest-dir',    help='specify the destination backup dir', dest='destdir', default=False, action='store', metavar="dir")
    parser.add_option('--backup-user',    help='specify the user to perform backups', dest='backupuser', default=False, action='store', metavar="dir")
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
    if opts.expire and not opts.sid:
        print "Option expire requires option sid"
        parser.print_help()
        exit(-1)
    if opts.removerun and not opts.sid:
        print "Option remove-run requires option sid"
        parser.print_help()
        exit(-1)
    if opts.removequeue and not opts.sid:
        print "Option remove-queue requires option sid"
        parser.print_help()
        exit(-1)
    if opts.addqueue and not opts.sid:
        print "Option add-queue requires option sid"
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
    if opts.backupuser:
        scheduler.backupuser = opts.backupuser
    if opts.show: # Displays pretty output of schedule, queue, and running tables
        scheduler = schedule()
        print scheduler
    elif opts.add: # Adds a schedule to the schedule table
        scheduler.newSchedule(time, backuptype, sourcehost, desthost, sourcedir, destdir)
    elif opts.remove: # Removes a single schedule from the schedules, removes all instances from queue and running
        scheduler.removeSchedule(scheduleid)
    elif opts.removerun: # Removes a single schedule from the queue
        scheduler.removeRunning(scheduleid)
    elif opts.removequeue: # Removes a single schedule from the queue
        scheduler.removeQueue(scheduleid)
    elif opts.removequeue: # Removes a single schedule from the queue
        scheduler.removeQueue(scheduleid)
    elif opts.expire: # Expires day in a schedule
        scheduler.expireSchedule(scheduleid)
    elif opts.addqueue: # Adds a single schedule to queue 
        scheduler.queueSchedule(scheduleid)
    elif opts.queue: # Searches and add all schedules not run today to queue, then moves them to running
        scheduler.queueSchedules()
        scheduler.startBackup()




if __name__ == '__main__':
    main()
