#!/usr/bin/env python 
import time
import string
import os
import sys
import datetime

thisdir = sys.path[0]
if thisdir=="":
       thisdir="."
if not thisdir.endswith("/"):
       thisdir=thisdir+"/"

logdir = os.getenv("LOG_DIR")
if not logdir.endswith("/"):
       logdir=logdir+"/"
print "Log dir = "+logdir
archivedir=logdir+"archive/"
print "Archive dir = "+archivedir
flagdir = os.getenv("FLAG_DIR")
if not flagdir.endswith("/"):
        flagdir=flagdir+"/"
print "Flag dir = "+flagdir

while 1:
        time.sleep(15)
        #print "reading available flags"
        try:
                availableFlags = os.listdir(flagdir)
        except:
                print "couldn't list flag dir"
                continue
        cleanUpFiles = {}
        doneFiles = {}
        for file in availableFlags:
                if file[-10:]==".flag.done":
                        #print "counting done file"
                        #remove .done
                        fileName = string.join(string.split(file,'.')[0:-1],'.')
                        #print file+": "+fileName
                        if doneFiles.has_key(fileName):
                                doneFiles[fileName] += 1
                        else:
                                doneFiles[fileName] = 1

        today=datetime.datetime.now().strftime('%Y%m%d')
        cmd = "/bin/mkdir -p "+archivedir+today
        if os.system(cmd)!=0:
                print "couldn't create archive directory "+archivedir+today
                continue

        for file in doneFiles.keys():
                #print "Looking to cleanup " + fileName
                print "cleaning up "+file
                cmd = "/bin/cat "+logdir+file+"*log | gzip -c > "+archivedir+today+"/"+file+".log.gz"
                if os.system(cmd)!=0:
                        print "couldn't consolidate log files for "+file
                else:
                        cmd = "/bin/rm -f "+logdir+file+"*log"
                        os.system(cmd)
                cmd = "/bin/mv "+logdir+file+"*gz "+archivedir+today
                if os.system(cmd)!=0:
                        print "couldn't move compressed log files for "+file
                        continue
                cmd = "/bin/rm -f "+flagdir+file+"*done "+flagdir+file+".cleanup"
                if os.system(cmd)!=0:
                        print "couldn't clear flags for "+file
        sys.stdout.flush()
