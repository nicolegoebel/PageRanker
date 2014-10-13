from time import strftime, time, localtime
#from MrPageRankIteration import MrPageRankIteration
from MrPageRankIteration2 import MrPageRankIteration2
import pdb
import sys
import random
import numpy as np
import csv
from pylab import plot, show, legend, savefig, xlabel, ylabel, title

def runJob(MRJobClass, argsArr, loc='local', cwd='not_set'):
    if loc == 'emc':
        argsArr.extend(['-r', 'emc'])
    #Extending arguments to include file option
    #The file will be available to all tasks (see class for details)
    #NOTE: This program assume a centroid file is available (initialization of centroids not covered here)
    paramsfile = cwd+'params.csv'
    argsArr.extend(['--params=%s' %paramsfile])
    #print "path set to: %s" %cwd
    #print "running: %s" %loc
    #print "damping parameter: %s" %dparam
    #print "argsArr: "
    #print argsArr
    #startTime=time()
    #print "%s starting %s job on %s" % (strftime("%a, %d %b %Y %H:%M::%S", localtime()), MRJobClass.__name__, loc)
    
    mrJob = MRJobClass(args=argsArr)
    runner = mrJob.make_runner()
    runner.run()

    # Dump output to pagerank file 
    # so next iteration knows what the latest pagranks are)
    f = open(datafile, 'w')
    for line in runner.stream_output():
        #print ('writing to final pagerank file')
        null, value = mrJob.parse_output_line(line)
        #print ('Writing, from Hadoop: ', value)
        #f.write(value.append('\n')
        f.write(str(value[0][0]) + ', ' + str(value[1]) +', ' + str(value[2]) + '\n')
        #f.write(str(value[0][0]) + ', ' + str(value[1][0]) + '\n')
    f.closed

    #endTime = time()
    #jobLength = endTime-startTime
    #print"%s finished %s job in %d seconds" % (strftime("%a, %d %b %Y %H:%M::%S", localtime()), MRJobClass.__name__, jobLength)

    return mrJob, runner

#Set path and data file name:
#Could pass as parameters, but to lazy to retype that stuff on the command line =)
cwd="/Users/nicolegoebel/Dropbox/Python_Projects/Homework_PageRank/"
if len(sys.argv)<2:
    fname="pagerank.csv"
else:
    fname=sys.argv[1]

runtype = "local"
datafile = cwd + fname
print "datafile: %s" %datafile
#------------ SPECIFY PARAMS HERE ----------------------------
tol = 0.0001
d=0.15   # damping parameter
n=11      # number of nodes (or pages)

pars=[d, n]
np.savetxt('params.csv', pars, delimiter=',')

n=500
for i in range (n):
    print('------------------- Start Iteration # %d -----------------------------' %i)
    ranks_in=[]
    with open(datafile,'rU') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        for row in reader:
            ranks_in.append(float(row[1]))
    csvfile.close()

    #get PRs_new
    #runJob(MrPageRankIteration, [datafile], runtype, cwd)
    runJob(MrPageRankIteration2, [datafile], runtype, cwd)

    ranks_out=[]
    with open(datafile,'rU') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        for row in reader:
            ranks_out.append(float(row[1]))
    csvfile.close()
    #get PRs_old
    PRsum=sum(ranks_out)
    print('sum of PageRanks=%.2f' %PRsum)
    dif = [abs(ranks_in - ranks_out) for ranks_in, ranks_out in zip(ranks_in, ranks_out)]
    mx = np.max(dif)#abs(set(ranks_in)-set(ranks_out)))
    if mx < tol:
        print ('tolerance reached')
        break
