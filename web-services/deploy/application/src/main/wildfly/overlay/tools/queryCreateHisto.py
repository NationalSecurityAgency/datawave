#!/usr/bin/python
import subprocess
import datetime
import bisect
import collections
from optparse import OptionParser


#-----------------defs----------------------
def printCmd(cmd):
  print "# Executing cmd: ", " ".join(cmd)

def analyzeCreates(fpath, YYYY, MM, DD, interval, numIntervalsInDay):
  start = datetime.datetime(int(YYYY), int(MM), int(DD), 0,0,0)
  grep = "fgrep"
  if fpath.endswith(".gz"):
    grep = "zgrep"
  cmd = [grep,"/DataWave/Query/create",fpath,"|fgrep",YYYY+"-"+MM+"-"+DD]
  printCmd(cmd)
  p = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
  out, err = p.communicate()
  interval = datetime.timedelta(minutes=interval)
  grid = [start+n*interval for n in range(numIntervalsInDay)]
  bins = {}

  for line in out.split("\n"):
    line = line.strip()
    if line:
      procRecord(line,grid,bins)
  return (bins, grid)

def procRecord(line, grid, bins):
    d, t = line.split()[0:2]
    t = t.split(",")[0] # strip off millis
    dt = parseTime(d,t) # yields datetime.datatime
    idx = bisect.bisect(grid,dt)
    updateDict(bins,idx)  

def parseTime(d,t):
  if ".gz" in d: # grep may leave filename prepended to output, strip it
    d = d.split(":")[1]
  d = [int(x) for x in d.split("-")]
  t = [int(x) for x in t.split(":")]
  return datetime.datetime(*tuple(d+t))

def updateDict(dict, idx):
  if idx in dict:
    dict[idx] += 1
  else:
    dict[idx] = 1



if __name__ == "__main__":
  # do stuff
  parser = OptionParser()
  parser.add_option("--interval", dest="interval", help="Time interval in minutes to bin create calls")
  parser.add_option("--date", dest="date", help="Date to investigate with format YYYYMMDD")
  (options,args) = parser.parse_args()

  if not (options.interval and options.date):
    print "Missing required parameter, see --help for usage."
    sys.exit(1)
  
  INTERVAL = int(options.interval) # in minutes
  NUM_INTERVALS = (86400 / (INTERVAL*60))+1 # num intervals in day

  YYYY = options.date[0:4]
  MM = options.date[4:6]
  DD = options.date[6:8]
  logName = "RestEasy.*"
  logPath = "/WebServiceLogs/" + YYYY + "/" + MM + "/" + DD + "/*/" 

  HADOOP_LS_CMD = ["hadoop","fs","-ls"]
  HADOOP_GET_CMD = ["hadoop","fs","-get"]

  cmd = HADOOP_LS_CMD + [logPath + logName]
  printCmd(cmd)

  p = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
  out, err = p.communicate()

  files=[]
  for line in out.split("\n"):
    if line.strip():
      path = line.split()[-1]
      parts = path.split("/")
      host = parts[-2]
      file = parts[-1]
      dest = "/tmp/"+host+"_"+file
      cmd = HADOOP_GET_CMD + [path, dest]
      printCmd(cmd)
      p = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
      out, err = p.communicate()
      files.append(dest)


  hostToBin={}

  for f in files:
    bins,grid = analyzeCreates(f,YYYY,MM,DD,INTERVAL,NUM_INTERVALS)
    hostToBin[f]=(bins,grid)

  hostKeys = hostToBin.keys()

  # Grab the first one so we can print out some stuff
  x = hostToBin.keys()[0]
  grid = hostToBin[x][1] # need grid fo the y-axis

  # Print csv of host names
  print ",".join(["Time/Host"]+hostKeys)

  for idx in range(0,len(grid)):
    line = [str(grid[idx])]
    for h in hostKeys:
      bins,g = hostToBin[h]
      
      if idx in bins:
        line.append(str(bins[idx]))
      else:
        line.append(str(0))

    print ",".join(line)


