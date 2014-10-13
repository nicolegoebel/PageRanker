from mrjob.job import MRJob
from mrjob.job import MRStep
import numpy as np
import pdb
import os
from collections import defaultdict

class MrPageRankIteration2(MRJob):
    
    # File option: The file specified will be available to each task local directory
    # and the value of the option will magically be changed to its path
    def configure_options(self):
        super(MrPageRankIteration2, self).configure_options()
        self.add_file_option('--params')

    def mapper(self, _, line):
        #print('Now in mapper')
        # read data (hadoop), ensures that each mapper gets some data
        # calculate key-value pairs for
        # (1) original page-outlinks
        # (2) outlinkA-PRshare
        # be sure to label key of (1) with a 2
        # be sure to label key of (2) with a 1+source page letter
        c = line.split('\r')
        #print c
        for i in range(len(c)):
            pg,rank, outlinks = c[i].split(',')
            #print pg[0]+'2', outlinks
            yield pg[0]+'2', outlinks
            for o in list(outlinks.strip()):
                if o in ('n','l','u'):
                    #print 'null'+'1'+pg, float(rank)/int(len(list(outlinks.strip())))
                    tele = float(rank)/int(len(list(outlinks.strip())))
                    yield 'null'+'1'+pg, float(rank)/int(len(list(outlinks.strip())))
                else:
                    #print o+'1'+pg, float(rank)/int(len(list(outlinks.strip())))
                    yield o+'1'+pg, (float(rank)/int(len(list(outlinks.strip()))))
                    try:
                        tele
                    except UnboundLocalError:
                        tele=0.0
                    yield o+'1'+'null', tele
            

    def reducer_init(self):
        # get d and N parameters (as for centroid problem)
        if not os.path.exists(self.options.params):
            file(self.options.params,'w').close()
        else:
            self.dpar, self.N = np.loadtxt(self.options.params, delimiter=',')#.tolist()

    def reducer(self, key, val):
        # Output from reducer:
        # Key: _   Value: N1 | New PR | N2,N3,N4 (passing PR and structure along)
        #print('Now in Reducer')
        l = list(val)[0]
        #print key, l
        if is_number(l):
            try:
                self.PRsum
            except AttributeError:
                self.PRsum=0.0
            #print ('key =%s' %key)
            #print ('PR to add=%.2f' %l)
            #print ('PRsum before adding =%.2f' %self.PRsum)
            self.PRsum+=l
            #print ('PRsum after adding =%.2f' %self.PRsum)
        else:
            PR = (self.dpar*(1.0/self.N)) + ((1-self.dpar) * self.PRsum)
            #print None, (key[0], PR, l)
            yield None, (key[0], PR, l)
            self.PRsum = 0.0
        
    def steps(self):
        return [MRStep(mapper=self.mapper,
            reducer_init=self.reducer_init,
            reducer=self.reducer)
            ]

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

if __name__ == '__main__':
    MrPageRankIteration2.run()
