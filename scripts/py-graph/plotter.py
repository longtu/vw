import numpy as np
from matplotlib.pyplot import *

def data(file1,lab):
    alphab1 = []
    frequencies1 = []
    f1 = open(file1, 'r')
    for line in f1:
        tokens = line.split()
        alphab1.append(float(tokens[0]))
        temp = (float(tokens[1])-float(tokens[3]))/float(tokens[1])
        frequencies1.append(float(temp))
    #print alphab1
    plot(alphab1, frequencies1,  label=lab)



data('threshold_60d_t0.85_b8_18_8hrs.data', 'diff')

legend(bbox_to_anchor=(0., 1.02, 1., .102), loc=3,
       ncol=4, mode="expand", borderaxespad=0.)
show()