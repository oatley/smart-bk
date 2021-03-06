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
#import string
import os
import re
import sqlite3 as lite
import smtplib
from email.mime.text import MIMEText

# Tools is used for logging to files and reporting to emails
class tools:
    def __init__(self):
        self.logdir = '/var/log/smart-bk/'

    # Log everything
    def writeLog(self, output):
        if isinstance(output, basestring):
            print str(output)
        else:
            for line in output:
                print str(line).strip()
        log = ""
        if len(str(self.day)) == 1:
            self.day = '0' + str(self.day)
        if len(str(self.minutes)) == 1:
            self.minutes = '0' + str(self.minutes)
        try:
            log = open(self.logdir + 'smart-bk-' + str(self.year) + '-' + str(self.month) + '-' + str(self.day) + '-' + str(self.hours) + '-' + str(self.minutes) + '.log', 'a+')
            if isinstance(output, basestring):
                log.write(str(output)+'\n')
            else:
                for line in output:
                    log.write(str(line))
        except Exception, e:
            print 'sbklog|error|' + str(e)
            pass
        finally:
            if log:
                log.close()
    
    # NEED TO REWRITE THIS FUNCTION, RECORD ALL SUCCESS AND FAILURE IN DATABASE not text file
    # Search through all log files from today, find if any tasks failed
    def showReport(self, date):
        subject = ''
        message = ''
        report = ['View backup website: http://iraq.proximity.on.ca/sbk/sbk.pl\n\n']
        log = ''
        status = 'success'
        logsbk = ''
        logstatus = ''
        logsid = ''
        logerror = ''
        logstart = ''
        logend = ''
        count = 0 # finished backups
        countrunning = 0 # started
        countqueue = 0 # queued
        countdisabled = 0 # queued
	countfailed = 0 # failed
        try:
            scheduler = schedule()
            total = len(scheduler.schedule)
            # Iterate through a list of files from directory
            for item in os.listdir(self.logdir):
                # Check if file has todays date
                if re.search('^smart-bk-' + date + '-.*' + '\.log$', item):
                    # Search through file for success and fails
                    log = open(self.logdir + item)
                    for line in log.readlines():
                        # Reset the id count in case you run a backup multiple times a day
                        if re.search('^sbklog\|success\|.*$', line):
                            logsbk, logstatus, logsid, logstart, logend = line.strip().split("|")
                            # Log status, success or failed, logsid, and take the description for the same logsid
                            report.append(logstatus + '\t\t| id = ' + logsid + ' - ' + scheduler.schedule[int(logsid)-1][-1])
                            count = count + 1
                        elif re.search('^sbklog\|failed\|.*$', line):
                            logsbk, logstatus, logsid, logstart, logend, logerror = line.strip().split("|")
                            # Log status, success or failed, logsid, and take the description for the same logsid
                            report.append(logstatus + '\t\t| id = ' + logsid + ' - ' + scheduler.schedule[int(logsid)-1][-1] + ' - ' + logerror)
                            status = 'failed'
                            countfailed = countfailed + 1
                        elif re.search('^sbklog\|disabled\|.*$', line):
                            logsbk, logstatus, logsid = line.strip().split("|")
                            # Log status, success or failed, logsid, and take the description for the same logsid
                            report.append(logstatus + '\t| id = ' + logsid + ' - ' + scheduler.schedule[int(logsid)-1][-1])
                            countdisabled = countdisabled + 1
                        elif re.search('^sbklog\|queue\|.*$', line):
                            countqueue = countqueue + 1
                        elif re.search('^sbklog\|running\|.*$', line):
                            countrunning = countrunning + 1
                        elif re.search('^sbklog\|error\|.*$', line):
                            status = 'failed'
            
            # sbk was run multiple times
            #if countrunning > count:
            #    report.append('Schedules Queued = ' + str(count) + '/' + str(countrunning) + '\n\n\n')
            #    report.append('Backups are still running\n')
            #else:
            #    report.append('Schedules Queued = ' + str(count) + '/' + str(countrunning) + '\n\n\n')
            report.append('-Backups Disabled = ' + str(countdisabled) + '\n')
            report.append('-Backups Queued = ' + str(countqueue) + '\n')
            report.append('-Backups Started = ' + str(countrunning) + '\n')
            report.append('-Backups Failed = ' + str(countfailed) + '\n')
            report.append('-Backups Successful = ' + str(count) + '\n')

            subject = 'Backup report - ' + status + ' - ' + date + '\n'
            message = ['[' + date + ']\n\n', subject + '\n', report]
        except Exception, e:
            print 'sbklog|error|(more reporting errors?) ' + str(e)
            pass
        finally:
            if log:
                log.close()
        return subject, message

    # Send out the report to email
    def sendReport(self, email, date):
        subject, message = self.showReport(date)
        fromemail = 'backup@proximity.on.ca'
        msg = MIMEText(message)
        msg['Subject'] = subject
        msg['From'] = fromemail
        msg['To'] = email
        #s = smtplib.SMTP('localhost')
        s = smtplib.SMTP('localhost')
        s.sendmail(fromemail, [email], msg.as_string())
        s.quit()

    # Check read and write permission of file
    def checkPermission(self, logfile):
        access = False
        if os.access(logfile, os.R_OK) and os.access(logfile, os.W_OK):
            access = True
        return access
    
    def outputSchedules(self):
        scheduler = schedule()
        for line in scheduler.schedule:
            print 'sbk --add ',\
                  ' --time="' + str(line[2]) + '"',\
                  ' --backup-type="' + str(line[3]) + '"',\
                  ' --source-host="' + str(line[4]) + '"',\
                  ' --dest-host="' + str(line[5]) + '"',\
                  ' --source-dir="' + str(line[6]) + '"',\
                  ' --dest-dir="' + str(line[7]) + '"',\
                  ' --source-user="' + str(line[8]) + '"',\
                  ' --dest-user="' + str(line[9]) + '"',\
                  ' --desc="' + str(line[10]) + '"'

    # Record logs into a database
    def recordLog(self, scheduleid, status, errors, starttime, endtime):
        #output = 'Adding scheduleid = ' + scheduleid + ' to logs'
        #self.writeLog(output)
        if scheduleid in self.queueids:
            return False
        try:
            con = lite.connect(self.database)
            cur = con.cursor()
            cur.execute('INSERT INTO logs(scheduleid, status, errors, start_time, end_time ) VALUES(?, ?, ?, ?, ?);', (scheduleid, status, errors, starttime, endtime))
            con.commit()
        except lite.Error, e:
            if con:
                con.rollback()
                #output = 'sbklog|error|' + e.args[0]
                #self.writeLog(output)
                exit()
        finally:
            if con:
                con.close()
        return True



# Schedule is used to completely manage schedules and backups
class schedule:
    def __init__(self):
        self.logdir = '/var/log/smart-bk/'
        self.database = '/data/smart-bk/schedule.db'
        self.updateTime()
        self.updateSchedules()

    # Update time
    def updateTime(self):
        # Get the date and time and store it
        self.now = str(datetime.datetime.now()).split(' ')
        self.year, self.month, self.day = self.now[0].split('-')
        self.hours, self.minutes, self.seconds = self.now[1].split(':')
        self.hours, self.minutes = int(self.hours), int(self.minutes)
        self.day = int(self.day)

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
        
    # Display updated schedules
    def displaySchedule(self):
        print '\nBusy Hosts:'
        for hosts in self.busyhosts:
            print '\t\t', hosts
        print 'Busy IDs:'
        for ids in self.busyids:
            print '\t\t', ids
        print 'Busy Schedules:'
        for schedules in self.busyschedules:
            print '\t\t', schedules
        print 'Free Hosts:'
        for hosts in self.freehosts:
            print '\t\t', hosts
        print 'Free IDs:'
        for ids in self.freeids:
            print '\t\t', ids
        print 'Free Schedules:'
        for schedules in self.freeschedules:
            print '\t\t', schedules
        print 'Queue Hosts:'
        for hosts in self.queuehosts:
            print '\t\t', hosts
        print 'Queue IDs:'
        for ids in self.queueids:
            print '\t\t', ids
        print 'Queue Schedules:'
        for schedules in self.queueschedules:
            print '\t\t', schedules


    # When you print object
    def __str__(self):
        return self.prettySchedule()
    
    # Create a new schedule
    def newSchedule(self, time, backuptype, sourcehost, desthost, sourcedir, destdir, sourceuser, destuser, desc):
        output = self.day, time, backuptype, sourcehost, desthost, sourcedir, destdir, sourceuser, destuser, desc
        self.writeLog(output)
        try:
            con = lite.connect(self.database)
            cur = con.cursor()
            cur.execute('INSERT INTO Schedule(day, time, type, source_host, dest_host, source_dir, dest_dir, source_user, dest_user, desc) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', (self.day, time, backuptype, sourcehost, desthost, sourcedir, destdir, sourceuser, destuser, desc))
            con.commit()
            con.close()
        except lite.Error, e:
            if con:
                con.rollback()
                con.close()
            output = 'sbklog|error|' + e.args[0]
            self.writeLog(output)
            exit()
    
    # Give a schedule id and delete that schedule
    def removeSchedule(self, scheduleid):
        output = 'Removing scheduleid = ' + str(scheduleid) + ' from all tables'
        self.writeLog(output)
        try:
            con = lite.connect(self.database)
            cur = con.cursor()
            cur.execute('DELETE FROM Queue WHERE scheduleid = ?;', [scheduleid])
            cur.execute('DELETE FROM Running WHERE scheduleid = ?;', [scheduleid])
            cur.execute('DELETE FROM Schedule WHERE id = ?;', [scheduleid])
            con.commit()
            con.close()
        except lite.Error, e:
            if con:
                con.rollback()
                con.close()
            output = 'sbklog|error|' + e.args[0]
            self.writeLog(output)
            exit()
    
    # Remove all schedules
    def removeSchedules(self):
        output = 'Removing ALL schedules from ALL tables'
        self.writeLog(output)
        output = 'I disabled this option because it seems too dangerous :p'
        self.writeLog(output)
        #for line in self.schedule:
        #    self.removeSchedule(line[0])

    # Output the schedule in a list
    def listSchedule(self):
        schedule = []
        queue = []
        running = []
        con = ''
        try:
            con = lite.connect(self.database)
            with con:
                cur = con.cursor()
                case = cur.execute('SELECT * FROM Schedule')
                rows = cur.fetchall()
                for row in rows:
                    # id, day, time, type, sourcehost, desthost, sourcedir, destdir, sourceuser, destuser, desc
                    schedule.append([row[0], row[1].strip(), row[2].strip(), row[3].strip(), row[4].strip(), row[5].strip(), row[6].strip(), row[7].strip(), row[8].strip(), row[9].strip(), row[10].strip()])
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
            output = 'sbklog|error|' + e.args[0]
            self.writeLog(output)
            exit()
    
    # Make the schedule look pretty and output it
    def prettySchedule(self):
        # Schedule
        print "\n\t" * 10 + "-[Schedule]-"
        print "-" * 100
        print "id" + "|" + "day" + "|" + "time" + "|" + "type" + "|" + "source host" + "|" + "dest host" + "|" + "source dir" + "|" + "dest dir" + "|" + "source user" + "|" + "dest user" + "|"
        print "-" * 100
        for item in self.schedule:
            print str(item[0]) + "|" + item[1] + "|" + item[2] + "|" + item[3] + "|" + item[4] + "|" + item[5] + "|" + item[6] + "|" + item[7] + "|" + item[8] + "|" + item[9]
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
        self.updateSchedules()
        time = ((self.day * 24) * 60 * 60) + (self.hours * 60 * 60) + (self.minutes * 60)
        output = "Checking all schedules for expired times"
        self.writeLog(output)
        for item in self.schedule:
            #print "real =" + self.month + " schedule=" 
            self.updateSchedules()
            shours, sminutes = item[2].split(':')
            shours, sminutes = int(shours), int(sminutes)
            lastday = int(item[1])
            schedtime = ((lastday * 24) * 60 * 60 ) + (shours * 60 * 60) + (sminutes * 60)
            scheduleid = item[0]
            if lastday == self.day:
                continue
            if lastday == 99:
                startdate, time = str(datetime.datetime.now()).split(' ')
                starttime, garbage = time.split('.')
                enddate, time = str(datetime.datetime.now()).split(' ')
                endtime, garbage = time.split('.')
                status = 'disabled'
                errors = ''
                output = 'sbklog|' + status + '|' + str(scheduleid)
                self.writeLog(output)
                self.recordLog(scheduleid, status, errors, startdate, starttime, enddate, endtime)
                continue
            if scheduleid in self.queueids:
                continue 
            if scheduleid in self.busyids:
                continue 
            # If the scheduled time has passed, move schedule into queue, also check the day
            if time >= schedtime or ( int(self.day) < int(lastday - 1)):
                output = 'Adding scheduleid = ' + str(scheduleid) + ' to queue'
                self.writeLog(output)
                output = 'sbklog|queue|' + str(scheduleid) + '|'
                self.writeLog(output)
                try:
                    # Add 0 for strings 1, 2, 3 to 01, 02, 03 - Important for minutes 12:03 looks like 12:3
                    if len(str(self.minutes)) == 1:
                        self.minutes = '0' + self.minutes
                    con = lite.connect(self.database)
                    cur = con.cursor()
                    cur.execute('INSERT INTO Queue(scheduleid, queuetime) VALUES(?, ?);', (scheduleid, str(self.hours) + ':' + str(self.minutes)))
                    cur.execute('UPDATE schedule SET day=? where id=?;', (str(self.day), scheduleid))
                    con.commit()
                except lite.Error, e:
                    if con:
                        con.rollback()
                    output = 'sbklog|error|' + e.args[0]
                    self.writeLog(output)
                    exit()
                finally:
                    if con:
                        con.close()
        return True

    def queueSchedule(self, scheduleid):
        output = 'Adding scheduleid = ' + scheduleid + ' to queue'
        self.writeLog(output)
        output = 'sbklog|queue|' + str(scheduleid) + '|'
        self.writeLog(output)
        if scheduleid in self.queueids:
            return False
        try:
            con = lite.connect(self.database)
            cur = con.cursor()
            cur.execute('INSERT INTO Queue(scheduleid, queuetime) VALUES(?, ?);', (scheduleid, str(self.hours) + ':' + str(self.minutes)))
            con.commit()
        except lite.Error, e:
            if con:
                con.rollback()
                output = 'sbklog|error|' + e.args[0]
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
            con = lite.connect(self.database)
            cur = con.cursor()
            cur.execute('UPDATE Schedule SET day=? where id = ?;', (str(int(self.day)-1), scheduleid))
            con.commit()
        except lite.Error, e:
            if con:
                con.rollback()
                output = 'sbklog|error|' + e.args[0]
                self.writeLog(output)
                exit()
        finally:
            if con:
                con.close()
        return True
   
    # Disable a schedule, prevents it from running
    def disableSchedule(self, scheduleid):
        output = 'Marking scheduleid = ' + scheduleid + ' as disabled'
        self.writeLog(output)
        try:
            con = lite.connect(self.database)
            cur = con.cursor()
            cur.execute('UPDATE Schedule SET day=? where id = ?;', (str(int(99)), scheduleid))
            con.commit()
        except lite.Error, e:
            if con:
                con.rollback()
                output = 'sbklog|error|' + e.args[0]
                self.writeLog(output)
                exit()
        finally:
            if con:
                con.close()
        return True

    # Clear entire queue
    def clearQueue(self):
        for scheduleid in self.queueids:
            self.removeQueue(str(scheduleid))

    # Delete a single queue
    def removeQueue(self, scheduleid):
        output = 'Deleting scheduleid = ' + scheduleid + ' from queue'
        self.writeLog(output)
        try:
            con = lite.connect(self.database)
            cur = con.cursor()
            cur.execute('DELETE FROM Queue WHERE scheduleid = ?', [scheduleid])
            con.commit()
        except lite.Error, e:
            if con:
                con.rollback()
            output = 'sbklog|error|' + e.args[0]
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
            con = lite.connect(self.database)
            cur = con.cursor()
            cur.execute('DELETE FROM Running WHERE scheduleid = ?', [scheduleid])
            con.commit()
        except lite.Error, e:
            if con:
                con.rollback()
            output = 'sbklog|error|' + e.args[0]
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
            con = lite.connect(self.database)
            cur = con.cursor()
            cur.execute('INSERT INTO running(scheduleid, starttime) VALUES(?, ?);', (scheduleid, str(self.hours) + ':' + str(self.minutes)))
            con.commit()
        except lite.Error, e:
            if con:
                con.rollback()
                output = 'sbklog|error|' + e.args[0]
                self.writeLog(output)
                exit()
        finally:
            if con:
                con.close()
        return True


    # Find all hosts in queue, find which one needs to be run first, move hosts to running if no conflicts
    def startBackup(self):
        #schedulerTools = tools()
        self.updateSchedules()
        hosts = []
        if not self.queueschedules:
                return False
        for row in self.queueschedules:
            self.updateSchedules()
            # UTC format YYYY-MM-DDThh:mm:ss
            #starttime, garbage = "T".join(str(datetime.datetime.now()).split(' ')).split('.')
            #endtime, garbage = "T".join(str(datetime.datetime.now()).split(' ')).split('.')
            startdate, time = str(datetime.datetime.now()).split(' ')
            starttime, garbage = time.split('.')
            enddate, time = str(datetime.datetime.now()).split(' ')
            endtime, garbage = time.split('.')
            # Easy to use variables for backups
            self.scheduleid = str(row[0])
            self.backuptype = row[3]
            self.sourcehost = row[4]
            self.desthost = row[5]
            self.sourcedir = row[6]
            self.destdir = row[7]
            self.sourceuser = row[8]
            self.destuser = row[9]
            # Check if this backup is already running
            if row in self.busyschedules:
                status = 'failed'
                errors = 'busy_scheduleid_aleady_running'
                output = 'sbklog|' + status + '|' + self.scheduleid + '|' + starttime + '|' + endtime + '|' + errors
                self.writeLog(output)
                self.recordLog(self.scheduleid, status, errors, startdate, starttime, enddate, endtime)
                continue
            if self.sourcehost in self.busyhosts or self.desthost in self.busyhosts:
                status = 'failed'
                errors = 'busy_scheduleid_busy_hosts=' + self.sourcehost + '/' +self.desthost
                output = 'sbklog|' + status + '|' + self.scheduleid + '|' + starttime + '|' + endtime + '|' + errors
                self.writeLog(output)
                self.recordLog(self.scheduleid, status, errors, startdate, starttime, enddate, endtime)
                continue
            # Check hosts for connectivity
            if not self.connectHost(self.sourcehost, self.sourceuser):
                status = 'failed'
                errors = 'no_connection_unavailable_host=' + self.sourcehost
                output = 'sbklog|' + status + '|' + self.scheduleid + '|' + starttime + '|' + endtime + '|' + errors
                self.writeLog(output)
                self.recordLog(self.scheduleid, status, errors, startdate, starttime, enddate, endtime)
                continue
            if not self.connectHost(self.desthost, self.destuser):
                status = 'failed'
                errors = 'no_connection_unavailable_host=' + self.desthost
                output = 'sbklog|' + status + '|' + self.scheduleid + '|' + starttime + '|' + endtime + '|' + errors
                self.writeLog(output)
                self.recordLog(self.scheduleid, status, errors, startdate, starttime, enddate, endtime)
                continue
            if self.sourcedir == self.destdir:
                status = 'error'
                output = 'sbklog|error|' + self.scheduleid + '|warning_sourcedir_overwriting_directories_same_name=' + self.sourcedir
                self.writeLog(output)
            hosts.append(self.sourcehost)
            hosts.append(self.desthost)
            self.removeQueue(self.scheduleid)
            self.newRunning(self.scheduleid)
            # Start the backup here 
            if self.backuptype == 'rsync':
                output = 'sbklog|running|' + self.scheduleid + '|rsync'
                self.writeLog(output)
                success = self.performRsync()
            elif self.backuptype == 'dbdump':
                output = 'sbklog|running|' + self.scheduleid + '|dbdump'
                self.writeLog(output)
                success = self.performDbdump()
            elif self.backuptype == 'archive':
                output = 'sbklog|running|' + self.scheduleid + '|archive'
                self.writeLog(output)
                success = self.performArchive()
            else:
                output = 'sbklog|error|' + self.scheduleid + '|unknown_backup_type=' + self.backuptype
                success = False
                self.writeLog(output)
            # Finish backup here
            self.removeRunning(self.scheduleid)
            #endtime, garbage = "T".join(str(datetime.datetime.now()).split(' ')).split('.')
            enddate, time = str(datetime.datetime.now()).split(' ')
            endtime, garbage = time.split('.')
            if success:
                #output = 'sbklog:success: ', row, '\n'
                status = 'success'
                errors = ''
                output = 'sbklog|' + status + '|' + self.scheduleid + '|' + starttime + '|' + endtime
                self.writeLog(output)
                self.recordLog(self.scheduleid, status, errors, startdate, starttime, enddate, endtime)
            else:
                status = 'failed'
                errors = 'error_unable_to_' + str(self.backuptype)
                output = 'sbklog|' + status + '|' + self.scheduleid + '|' + starttime + '|' + endtime + '|' + errors
                self.writeLog(output)
                self.recordLog(self.scheduleid, status, errors, startdate, starttime, enddate, endtime)
        return True 
    
    # Record logs into a database
    def recordLog(self, scheduleid, status, errors, startdate, starttime, enddate, endtime):
        output = 'Adding scheduleid = ' + str(scheduleid) + ' to logs'
        self.writeLog(output)
        if scheduleid in self.queueids:
            return False
        try:
            con = lite.connect(self.database)
            cur = con.cursor()
            cur.execute('INSERT INTO logs(scheduleid, status, errors, start_date, start_time, end_date, end_time ) VALUES(?, ?, ?, ?, ?, ?, ?);', (str(scheduleid), status, errors, startdate, starttime, enddate, endtime))
            con.commit()
        except lite.Error, e:
            if con:
                con.rollback()
                output = 'sbklog|error|' + e.args[0]
                self.writeLog(output)
                exit()
        finally:
            if con:
                con.close()
        return True


    # Log everything
    def writeLog(self, output):
        if isinstance(output, basestring):
            print str(output)
        else:
            for line in output:
                print str(line).strip()
        log = ""
        if len(str(self.day)) == 1:
            self.day = '0' + str(self.day)
        if len(str(self.minutes)) == 1:
            self.minutes = '0' + str(self.minutes)
        try:
            log = open(self.logdir + 'smart-bk-' + str(self.year) + '-' + str(self.month) + '-' + str(self.day) + '-' + str(self.hours) + '-' + str(self.minutes) + '.log', 'a+')
            if isinstance(output, basestring):
                log.write(str(output)+'\n')
            else:
                for line in output:
                    log.write(str(line))
        except Exception, e:
            print "sbklog|error|" + str(e)
            pass
        finally:
            if log:
                log.close()
             
    # Connect to the hosts, return True if success or False if not successful
    def connectHost(self, host, user):
        try:
            #response=urllib2.urlopen('http://'+host,timeout=1)
            srv = pysftp.Connection(host=host, username=user, log=True)
            srv.close()
            return True
        #except urllib2.URLError as err:pass
        except:pass
        return False

    # Check disk space of partition/lv where directory resides
    def availableSpace(self, scheduleid):
        output = ""
        errors = True
        srv = ""
        for row in self.schedule:
            if str(row[0]) == str(scheduleid):
                sourcehost = row[4]
                desthost = row[5]
                sourcedir = row[6]
                destdir = row[7]
                sourceuser = row[8]
                destuser = row[9]
                errors = False
        if errors:
            return False
        try:
            srv = pysftp.Connection(host=sourcehost, username=sourceuser, log=True)
            output = "df " + sourcedir + " | awk '{print $4}' | grep '^[0-9]*$'"
            self.writeLog(output)
            output = srv.execute("df " + sourcedir + " | awk '{print $4}' | grep '^[0-9]*$'")
            self.writeLog(output)
            self.usedSpace(sourceuser, sourcehost, sourcedir)
            srv = pysftp.Connection(host=desthost, username=destuser, log=True)
            output = "df " + destdir + " | awk '{print $4}' | grep '^[0-9]*$'"
            self.writeLog(output)
            output = srv.execute("df " + destdir + " | awk '{print $4}' | grep '^[0-9]*$'")
            self.writeLog(output)
            self.usedSpace(destuser, desthost, destdir)
        except Exception, e:
            if output:
                self.writeLog(output)
            errors = "sbklog|error|" + str(e)
            self.writeLog(errors)
            pass
        finally:
            if srv:
                srv.close()
        if errors:
            return False
        return True
    
    # Check total space that this directory uses
    def usedSpace(self, user, host, directory):
        output = ""
        errors = ""
        srv = ""
        try:
            srv = pysftp.Connection(host=host, username=user, log=True)
            output = "du -s " + directory + " | awk '{print $1}' | grep '^[0-9]*$'"
            self.writeLog(output)
            output = srv.execute("du -s " + directory + " | awk '{print $1}' | grep '^[0-9]*$'")
            self.writeLog(output)
        except Exception, e:
            if output:
                self.writeLog(output)
            errors = "sbklog|error|" + str(e)
            self.writeLog(errors)
            pass
        finally:
            if srv:
                srv.close()
        if errors:
            return False
        return output
             
                                                                                                
    # Backup is complete, clean, log, and email results
    def performRsync(self):
        output = ""
        errors = ""
        srv = ""
        try:
            srv = pysftp.Connection(host=self.sourcehost, username=self.sourceuser, log=True)
            output = 'sudo rsync -aHXvz --exclude "lost+found" ' + self.sourcedir + ' ' + self.destuser + '@' + self.desthost + ':' + self.destdir
            self.writeLog(output)
            output = srv.execute('sudo rsync -aHAXEvz --exclude "lost+found" ' + self.sourcedir + ' ' + self.destuser + '@' + self.desthost + ':' + self.destdir + ';echo $?')
            self.writeLog(output)
            if output[-1].strip() != '0':
                errors = 'sbklog|error|command returned a non-zero exit status'
                self.writeLog(errors)
            srv.close()
        except Exception, e:
            if output:
                self.writeLog(output)
            errors = "sbklog|error|" + str(e)
            self.writeLog(errors)
            pass
        finally:
            if srv:
                srv.close()
        if errors:
            return False
        return True


    # Perfom a archive
    def performArchive(self):
        tarfile = '/tmp/archive-' + str(self.year) + '-' + str(self.month) + '-' + str(self.day) + '-' + str(self.hours) + '-' + str(self.minutes) + '.tar.bz'
        output = ""
        errors = ""
        srv = ""
        try:
            srv = pysftp.Connection(host=self.sourcehost, username=self.sourceuser, log=True)
            output = 'sudo tar -cpjvf ' + tarfile + ' ' + self.sourcedir
            self.writeLog(output)
            output = srv.execute('sudo tar -cpjvf ' + tarfile + ' ' + self.sourcedir + ';echo $?')
            self.writeLog(output)
            if output[-1].strip() != '0':
                errors = 'sbklog|error|command returned a non-zero exit status'
                self.writeLog(errors)
                #raise Exception(errors)
            output = 'scp ' + tarfile + ' ' + self.destuser + '@' + self.desthost + ':' + self.destdir
            self.writeLog(output)
            output = srv.execute('scp ' + tarfile + ' ' + self.destuser + '@' + self.desthost + ':' + self.destdir + ';echo $?')
            self.writeLog(output)
            if output[-1].strip() != '0':
                errors = 'sbklog|error|command returned a non-zero exit status'
                self.writeLog(errors)
                #raise Exception(errors)
            output = 'rm -f' + tarfile
            self.writeLog(output)
            output = srv.execute('rm -f ' + tarfile)
            self.writeLog(output)
            srv.close()
        except Exception, e:
            if output:
                self.writeLog(output)
            errors = "sbklog|error|" + str(e)
            self.writeLog(errors)
            pass
        finally:
            if srv:
                srv.close()
        if errors:
            return False
        return True

    # Perform a dbdump on koji... Should probably change this to support a custom db name
    def performDbdump(self):
        kojifile = '/tmp/kojidb-' + str(self.year) + '-' + str(self.month) + '-' + str(self.day) + '-' + str(self.hours) + '-' + str(self.minutes) + '.sql'
        output = ""
        errors = ""
        srv = ""
        try:
            srv = pysftp.Connection(host=self.sourcehost, username=self.sourceuser, log=True)
            output = 'pg_dump koji > ' + kojifile
            self.writeLog(output)
            output = srv.execute('pg_dump koji > ' + kojifile + ';echo $?')
            self.writeLog(output)
            if output[-1].strip() != '0':
                errors = 'sbklog|error|command returned a non-zero exit status'
                self.writeLog(errors)
            output = 'scp ' + kojifile + ' ' + self.destuser + '@' + self.desthost + ':' + self.destdir
            self.writeLog(output)
            output = srv.execute('scp ' + kojifile + ' ' + self.destuser + '@' + self.desthost + ':' + self.destdir + ';echo $?')
            self.writeLog(output)
            if output[-1].strip() != '0':
                errors = 'sbklog|error|command returned a non-zero exit status'
                self.writeLog(errors)
        except Exception, e:
            if output:
                self.writeLog(output)
            errors = "sbklog|error|" + str(e)
            self.writeLog(errors)
            pass
        finally:
            if srv:
                srv.close()
        if errors:
            return False
        return True


def main():
    # Create command line options
    desc = """The smart backup scheduler program %prog is used to run backups from computer to computer. %prog does this by adding and removing schedules
from a schedule database. Once added to the schedule database, %prog should be run with '--queue' in order to intelligently
add hosts to a queue and start running backups. It is recommended to run this as a cron job fairly often, more fequently
depending on the number of schedules."""
    parser = optparse.OptionParser(description=desc, usage='Usage: %prog [options]')
    parser.add_option('-q', '--queue',    help='queue schedules and start backups', dest='queue', default=False, action='store_true')
    parser.add_option('-a', '--add',    help='add new schedule at specific time', dest='add', default=False, action='store_true')
    parser.add_option('-s', '--show',    help='show the schedule and host info', dest='show', default=False, action='store_true')
    parser.add_option('-r', '--remove',    help='remove existing schedule', dest='remove', default=False, action='store_true')
    parser.add_option('-d', '--display-hosts',    help='display busy and free hosts', dest='displayhosts', default=False, action='store_true')
    parser.add_option('--remove-run',    help='remove existing schedule from running', dest='removerun', default=False, action='store_true')
    parser.add_option('--remove-queue',    help='remove existing schedule from queue', dest='removequeue', default=False, action='store_true')
    parser.add_option('--remove-all',    help='remove all schedules', dest='removeall', default=False, action='store_true')
    parser.add_option('--clear-queue',    help='remove all schedules from queue', dest='clearqueue', default=False, action='store_true')
    parser.add_option('--expire',    help='expire the day in schedule', dest='expire', default=False, action='store_true')
    parser.add_option('--disable-schedule',    help='stop a schedule from running', dest='disableschedule', default=False, action='store_true')
    parser.add_option('--enable-schedule',    help='allow schedule to run again', dest='enableschedule', default=False, action='store_true')
    parser.add_option('--add-queue',    help='add a single schedule to queue', dest='addqueue', default=False, action='store_true')
    parser.add_option('--sid',    help='specify schedule id for removing schedules', dest='sid', default=False, action='store', metavar="scheduleid")
    parser.add_option('--time',    help='specify the time to run the backup', dest='time', default=False, action='store', metavar="18:00")
    parser.add_option('--backup-type',    help='archive, pg_dump, rsync', dest='backuptype', default=False, action='store', metavar="type")
    parser.add_option('--source-host',    help='specify the source backup host', dest='sourcehost', default=False, action='store', metavar="host")
    parser.add_option('--source-dir',    help='specify the source backup dir', dest='sourcedir', default=False, action='store', metavar="dir")
    parser.add_option('--source-user',    help='specify the source user', dest='sourceuser', default=False, action='store', metavar="user")
    parser.add_option('--dest-host',    help='specify the destination backup host', dest='desthost', default=False, action='store', metavar="host")
    parser.add_option('--dest-dir',    help='specify the destination backup dir', dest='destdir', default=False, action='store', metavar="dir")
    parser.add_option('--dest-user',    help='specify the destination user', dest='destuser', default=False, action='store', metavar="user")
    parser.add_option('--desc',    help='specify a short description for backup', dest='desc', default=False, action='store', metavar="desc")
    parser.add_option('--log-dir',    help='specify the directory to save logs', dest='logdir', default=False, action='store', metavar="dir")
    parser.add_option('--check-disk',    help='check disk space on directory and volume', dest='checkdisk', default=False, action='store_true')
    parser.add_option('--show-report',    help='show report for date specified', dest='showreport', default=False, action='store_true')
    parser.add_option('--send-report',    help='send report to email', dest='sendreport', default=False, action='store_true')
    parser.add_option('--report-date',    help='specify the date for report checking', dest='reportdate', default=False, action='store', metavar="yyyy-mm-dd")
    parser.add_option('--report-email',    help='specify the email to send report', dest='reportemail', default=False, action='store', metavar="a@b.ca")
    parser.add_option('--save-schedules',    help='print out schedules in bash script', dest='saveschedules', default=False, action='store_true')
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
    if opts.sourceuser:
        sourceuser = opts.sourceuser
    if opts.destuser:
        destuser = opts.destuser
    if opts.desc:
        desc = opts.desc
    if opts.reportdate:
        reportdate = opts.reportdate
    if opts.reportemail:
        reportemail = opts.reportemail

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
    if opts.checkdisk and not opts.sid:
        print "Option remove-queue requires option sid"
        parser.print_help()
        exit(-1)
    if opts.addqueue and not opts.sid:
        print "Option add-queue requires option sid"
        parser.print_help()
        exit(-1)
    if opts.disableschedule and not opts.sid:
        print "Option disable-schedule requires option sid"
        parser.print_help()
        exit(-1)
    if opts.enableschedule and not opts.sid:
        print "Option enable-schedule requires option sid"
        parser.print_help()
        exit(-1)
    if opts.add:
        if not opts.time or not opts.backuptype or not opts.sourcehost or not opts.desthost or not opts.sourcedir or not opts.destdir or not opts.sourceuser or not opts.destuser or not opts.desc:
            print "Option add requires option time, backup-type, source-host, dest-host, source-dir, dest-dir, source-user, dest-user, desc"
            parser.print_help()
            exit(-1)
    if opts.showreport and not opts.reportdate:
        print "Option show-report requires option report-date"
        parser.print_help()
        exit(-1)
    if opts.sendreport:
        if not opts.reportemail or not opts.reportdate:
            print "Option send-report requires option report-email, report-date"
            parser.print_help()
            exit(-1)

    # Weird use cases
    if opts.add and opts.remove:
        parser.print_help()
        exit(-1)

    # Start program
    scheduler = schedule()
    schedulerTools = tools()
    if opts.logdir:
        scheduler.logdir = opts.logdir
    if opts.show: # Displays pretty output of schedule, queue, and running tables
        scheduler = schedule()
        print scheduler
    elif opts.displayhosts:
        scheduler.displaySchedule()
    elif opts.showreport:
        subject, report = schedulerTools.showReport(reportdate)
        #print report
        for item in report:
            if type(item) is list:
                item.sort(key=lambda x: x[1])
                item.sort(key=lambda x: x[0])
                for schd in item:
                    print ''.join(str(i) for i in schd).strip()
            else:
                print item
    elif opts.sendreport:
        print reportemail
        print reportdate
        schedulerTools.sendReport(reportemail, reportdate)
    elif opts.add: # Adds a schedule to the schedule table
        scheduler.newSchedule(time, backuptype, sourcehost, desthost, sourcedir, destdir, sourceuser, destuser, desc)
    elif opts.remove: # Removes a single schedule from the schedules, removes all instances from queue and running
        scheduler.removeSchedule(scheduleid)
    elif opts.removerun: # Removes a single schedule from the queue
        scheduler.removeRunning(scheduleid)
    elif opts.removequeue: # Removes a single schedule from the queue
        scheduler.removeQueue(scheduleid)
    elif opts.clearqueue: # Removes a single schedule from the queue
        scheduler.clearQueue()
    elif opts.expire: # Expires day in a schedule
        scheduler.expireSchedule(scheduleid)
    elif opts.addqueue: # Adds a single schedule to queue 
        scheduler.queueSchedule(scheduleid)
    elif opts.queue: # Searches and add all schedules not run today to queue, then moves them to running
        scheduler.queueSchedules()
        scheduler.startBackup()
    elif opts.checkdisk: # Check disk space
        scheduler.availableSpace(scheduleid)
    elif opts.disableschedule: # Disable schedule
        scheduler.disableSchedule(scheduleid)
    elif opts.enableschedule: # Disable schedule
        scheduler.expireSchedule(scheduleid)
    elif opts.saveschedules: # output schedules in bash
        schedulerTools.outputSchedules()
    elif opts.removeall: # output schedules in bash
        schedulerTools.outputSchedules()
        scheduler.removeSchedules()



if __name__ == '__main__':
    main()
