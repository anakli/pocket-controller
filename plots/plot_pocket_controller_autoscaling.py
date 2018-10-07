import pandas as pd
import sys
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from matplotlib.text import OffsetFrom

#python plot_pocket_controller_autoscaling.py sort1-video-sort2-WORKS-util-v2.log
#time net_usedMbps avg_cpu dram_usedGB net_allocMbps dram_allocGB
plt.rcParams.update({'font.size': 24})
plt.rcParams.update({'pdf.fonttype': 42})
plt.rcParams.update({'ps.fonttype': 42})

def plot_usage(logfile):
  data = pd.read_csv(logfile, sep=' ') # header=True) #, skipinitialspace=True)
  print(list(data))
  start_time = data.at[0, 'time']
  data['time'] = data['time'] - start_time
 
  REGISTER_JOB1 = 1525312251.8904905 - start_time
  DEREGISTER_JOB1 =  1525312310.2906423 - start_time
  REGISTER_JOB2 = 1525312332.0015373 - start_time
  REGISTER_JOB3 = 1525312376.7232 - start_time
  DEREGISTER_JOB3 = 1525312443.3627393 - start_time
  DEREGISTER_JOB2 = 1525312542.2918663 - start_time
  x = data.loc[:,'time']
  net_usage = data.loc[:,'net_usedMbps'] / (8*1e3)
  net_alloc = data.loc[:,'net_allocMbps'] / (8*1e3) #/ 8 * 10
  cpu = data.loc[:, 'avg_cpu']
  dram_usedGB = data.loc[:,'dram_usedGB']
  dram_allocGB = data.loc[:, 'dram_allocGB']


  fig = plt.figure(figsize=(15,8))
  ax = plt.axes([0.06, 0.2, 0.9, 0.75]) # left bottom width height (fraction of total figsize)
  ax.plot(x, net_alloc, label='Total GB/s allocated', linestyle='--', color="#1f77b4", linewidth=4)
  ax.plot(x, net_usage, label='Total GB/s used', color="#ff7f0e", linewidth=4)
  ax.set_xlabel("Time (s)")
  ax.set_ylabel("Throughput (GB/s)")
  ax.legend(loc='upper left')

  ax.annotate('Job1', xy=(REGISTER_JOB1, -0.07), xytext=(REGISTER_JOB1, -0.2),
            xycoords=('data', 'axes fraction'), textcoords=('data', 'axes fraction'),
            va='center',ha='center',color='blue',
            arrowprops=dict(arrowstyle='->', color='blue'), size=18
            )
  ax.annotate('Job1', 
            xy=(DEREGISTER_JOB1, -0.07), xytext=(DEREGISTER_JOB1, -0.2), 
            xycoords=('data', 'axes fraction'), textcoords=('data', 'axes fraction'),
            va='center',ha='center', color='blue',
            arrowprops=dict(arrowstyle='<-', color='blue'), size=18
            )
  ax.annotate('Job2', xy=(REGISTER_JOB2, -0.07), xytext=(REGISTER_JOB2, -0.2),
            xycoords=('data', 'axes fraction'), textcoords=('data', 'axes fraction'),
            va='center',ha='center',color='green',
            arrowprops=dict(arrowstyle='->', color='green'), size=18
  )
  ax.annotate('Job3', xy=(REGISTER_JOB3, -0.07), xytext=(REGISTER_JOB3, -0.2),
            xycoords=('data', 'axes fraction'), textcoords=('data', 'axes fraction'),
            va='center',ha='center',color='grey',
            arrowprops=dict(arrowstyle='->',color='grey',), size=18
  )
  ax.annotate('Job3', xy=(DEREGISTER_JOB3, -0.07), xytext=(DEREGISTER_JOB3, -0.2),
            xycoords=('data', 'axes fraction'), textcoords=('data', 'axes fraction'),
            va='center',ha='center',color='grey',
            arrowprops=dict(arrowstyle='<-',color='grey',), size=18
  )
  ax.annotate('Job2', xy=(DEREGISTER_JOB2, -0.07), xytext=(DEREGISTER_JOB2, -0.2),
            xycoords=('data', 'axes fraction'), textcoords=('data', 'axes fraction'),
            va='center',ha='center',color='green',
            arrowprops=dict(arrowstyle='<-',color='green',), size=18
  )


  #plt.show()
  plt.savefig("pocket_controller_autoscale-8Gbs-.pdf")


if __name__ == '__main__':
  logfile = sys.argv[1]
  plot_usage(logfile)
  
