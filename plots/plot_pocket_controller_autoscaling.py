import pandas as pd
import sys
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

#time net_usedMbps avg_cpu dram_usedGB net_allocMbps dram_allocGB


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
  net_alloc = data.loc[:,'net_allocMbps'] / (8*1e3)
  cpu = data.loc[:, 'avg_cpu']
  dram_usedGB = data.loc[:,'dram_usedGB']
  dram_allocGB = data.loc[:, 'dram_allocGB']

  fig = plt.figure()
  ax = fig.add_subplot(111)
  ax.plot(x, net_alloc, label='Total GB/s allocated', linestyle=':', color="#ff7f0e")
  ax.plot(x, net_usage, label='Total GB/s used', color="#ff7f0e")
  ax.set_xlabel("Time (s)")
  ax.set_ylabel("Throughput (GB/s)")
  ax.legend(loc='upper left')

  #ax.annotate('Job1 registers', xy=(REGISTER_JOB1, 2), xytext=(REGISTER_JOB1, 1.5),
  #          arrowprops=dict(facecolor='black', shrink=0.05), horizontalalignment='left', verticalalignment='bottom'
  #          )
  #ax.annotate('Job1 deregisters', xy=(DEREGISTER_JOB1, 3), xytext=(DEREGISTER_JOB1, 3.5),
  #          arrowprops=dict(facecolor='black', shrink=0.05), horizontalalignment='right', verticalalignment='top'
  #          )
  
  ax1 = plt.figure().add_subplot(111)
  ax1.plot(x, dram_allocGB, label='Total GB allocated', linestyle=':', color="#1f77b4")
  ax1.plot(x, dram_usedGB, label='Total GB used', color="#1f77b4")
  ax1.set_xlabel("Time (s)")
  ax1.set_ylabel("Capacity (GB)")
  ax1.legend(loc='upper left')

  plt.show()


if __name__ == '__main__':
  logfile = sys.argv[1]
  plot_usage(logfile)
  
