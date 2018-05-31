import numpy as np
import matplotlib.pyplot as plt

plt.rcParams.update({'font.size': 24})
fig, ax = plt.subplots(figsize=(20, 7))

N = 3 # number of groups on x-axis

ind = np.arange(N)  # the x locations for the groups
#ind = np.array([0.8,1])
print ind
width = 0.25      # the width of the bars
w = 0.05

#           250         500
reflex_1 = [0.6846274676,
        1.346892113,
        2.619937357] #map compute
reflex_1.reverse()
reflex_2 = [9.506996859,
        4.443747664,
        2.64235723]  #write
reflex_3 = [11.27883145,
        4.055369335,
        1.712588097] #read
reflex_4 = [3.00149984,
        6.65288902,
        14.21705652] #reduce compute
reflex_4.reverse()
reflex_data=[reflex_1, reflex_2, reflex_3, reflex_4]
reflex=[] #8x25Gbps
for i in xrange(4):
    reflex.append(reflex_data[i])
reflex_bottom = []
tmp = [0,0,0]
for i in reflex:
    tmp = np.array(i)+np.array(tmp)
    reflex_bottom.append(tmp)



#           250         500
dram_1 = [15.02261312, 0] #input/output
dram_2 = [16.78998469, 0]  #compute
dram_3 = [27.65068124, 0] #inter read/write
dram_data=[dram_1, dram_2, dram_3]
dram=[] #8x25Gbps
for i in xrange(3):
    dram.append(dram_data[i])
dram_bottom = []
tmp = [0,0]#,0]
for i in dram:
    tmp = np.array(i)+np.array(tmp)
    dram_bottom.append(tmp)




#		250	500		1000 workers
redis_1 = [0.6964159946,
        1.363046318,
        2.62584041]
redis_1.reverse()
redis_2 = [1.905451184,
        5.78952875,
        10.86712596] #compute
redis_2.reverse()
redis_3 = [1.137173061,
        2.208369201,
        6.116519001] #inter read/write
redis_3.reverse()
redis_4 = [2.975205686,
        6.468844582,
        14.42502981]
redis_4.reverse()
redis_data=[redis_1, redis_2, redis_3, redis_4]
redis=[] #8x25Gbps
for i in xrange(4):
    redis.append(redis_data[i])
redis_bottom = []
tmp = [0,0,0]
for i in redis:
    tmp = np.array(i)+np.array(tmp)
    redis_bottom.append(tmp)


s3_1=[2.487143972, 0,0]#, 0]
s3_2=[33.27539105,0,0]#,0]
s3_3=[27.03011903,0,0]#,0]
s3_4=[13.63355405,0,0]
s3=[]
s3.append(s3_1)
s3.append(s3_2)
s3.append(s3_3)
s3.append(s3_4)
s3_bottom = []
tmp = [0,0,0]
for i in s3:
    tmp = np.array(i)+np.array(tmp)
    s3_bottom.append(tmp)

#plt.rcParams.update({'font.size': 24})
#plt.rcParams.update({'font.size': 28})
#fig, ax = plt.subplots()
#fig, ax = plt.subplots(figsize=(18, 10))
#fig, ax = plt.subplots(figsize=(16, 8))
##fig.tight_layout()

c_b = '#1f77b4'
c_r = '#d62728'
c_y = '#bcbd22'
c_o = '#ff7f0e'
c_g = '#2ca02c'
c_p = '#9467bd'
c = [c_b, c_o, c_y, c_p, c_r, c_g]
p = []
h = ['/','.','x','//','-','o']

ind_0 = ind-(width)/2-w
ind_1 = ind+(width)/2
ind_2 = ind+(width)*3/2+w

p0 = ax.bar(ind_0, s3_1, width, color=c_r, hatch='/')
p0_1 = ax.bar(ind_0, s3_2, width, color=c_b, hatch='.', bottom=s3_bottom[0])
p0_2 = ax.bar(ind_0, s3_3, width, color=c_o, hatch='x', bottom=s3_bottom[1])
p0_3 = ax.bar(ind_0, s3_4, width, color=c_g, hatch='o', bottom=s3_bottom[2])

p1 = ax.bar(ind_1, redis_1, width, color=c_r,hatch='/')
p1_1 = ax.bar(ind_1, redis_2, width, color=c_b, hatch='.', bottom=redis_bottom[0])
p1_2 = ax.bar(ind_1, redis_3, width, color=c_o, hatch='x', bottom=redis_bottom[1])
p1_3 = ax.bar(ind_1, redis_4, width, color=c_g, hatch='o', bottom=redis_bottom[2])
'''
ind_2 = ind+width+w/2
ind_3 = ind+(width+w)*2
'''
p2 = ax.bar(ind_2, reflex_1, width, color=c_r, hatch='/')
p2_1 = ax.bar(ind_2, reflex_2, width, color=c_b, hatch='.', bottom=reflex_bottom[0])
p2_2 = ax.bar(ind_2, reflex_3, width, color=c_o, hatch='x', bottom=reflex_bottom[1])
p2_3 = ax.bar(ind_2, reflex_4, width, color=c_g, hatch='o', bottom=reflex_bottom[2])
'''
p3 = ax.bar(ind_2, dram_1, width, color='b')
p3_1 = ax.bar(ind_2, dram_2, width, color='g', bottom=dram_bottom[0])
p3_2 = ax.bar(ind_2, dram_3, width, color='#FDA96A', bottom=dram_bottom[1])
'''

'''
n=2
p2 = ax.bar(ind+(width+w)*n, redis10_1, width, color='b')
p2_1 = ax.bar(ind+(width+w)*n, redis10_2, width, color='g', bottom=redis10_bottom[0])
p2_2 = ax.bar(ind+(width+w)*n, redis10_3, width, color='#FDA96A', bottom=redis10_bottom[1])

n = 3
p3 = ax.bar(ind+(width+w)*n, redis25_1, width, color='b')
p3_1 = ax.bar(ind+(width+w)*n, redis25_2, width, color='g', bottom=redis25_bottom[0])
p3_2 = ax.bar(ind+(width+w)*n, redis25_3, width, color='#E4A000', bottom=redis25_bottom[1])
'''


#ax.set_title('100GB Sort')
ax.set_ylabel('Average Time per Lambda (s)')
ax.set_xticks(ind + width / 2)
#ax.set_xticklabels(('           S3      Redis  Pocket-Flash \n250 Workers', '         S3           Redis   Pocket-Flash \n500 Workers', '           S3      Redis  Pocket-Flash \n1000 Workers'))
ax.set_xticklabels(('           S3      Redis  Crail-ReFlex \n250 lambdas', '                      Redis   Crail-ReFlex \n500 lambdas', '                      Redis   Crail-ReFlex \n1000 lambdas'), fontsize=20)
#ax.set_xlabel('# of Lambdas Workers')
ax.legend((p1[0], p1_3[0], p1_1[0], p1_2[0]), ('Map Compute','Reduce Compute','I/O Write','I/O Read'),fontsize=24)
#ax.legend((p1[0], p1_1[0], p0_2[0], p1_2[0], p2_2[0], p3_2[0]), ('Input/Ouput','Compute','S3 R/W','Redis R/W 8x25Gbps', 'Redis R/W 4x10Gbps', 'Redis R/W 4x25Gbps'))
plt.show()

#for tick in ax.get_xticklabels():
#    tick.set_rotation(45)
fig.savefig("sort-barplot-atc18.pdf")
