import pandas as pd
import sys
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

#time net_usedMbps avg_cpu dram_usedGB net_allocMbps dram_allocGB

#python3 plot_gc.py videoanalytics-4datanode-resource_util-noGC-redo.log videoanalytics-4datanode-resource_util-withGC-redo.log
 
plt.rcParams.update({'font.size': 24})


def plot_usage(noGC, withGC):
  dataGC = pd.read_csv(withGC, sep=' ')
  #dataGC = dataGC[(dataGC.loc[:,'dram_usedGB'] != 0).any()]
  #dataGC = dataGC.drop(dataGC.index[[80,180]])
  dataGC = dataGC.drop(dataGC.index[0:18]).reset_index()
  dataGC = dataGC.drop(dataGC.index[100:150]).reset_index()
  #dataGC = dataGC.drop(dataGC.index[180:190]).reset_index()
  #dataGC = dataGC.iloc[4:].reset_index()
  start_time = dataGC.at[0, 'time']
  dataGC['time'] = dataGC['time'] - start_time
 
   
  #x_GC = dataGC.loc[:,'time']
  x_GC = range(0, len(list(dataGC.index.values))) #dataNoGC.loc[:,'time']
  dram_used_GC = dataGC.loc[:,'dram_usedGB']

  dataNoGC = pd.read_csv(noGC, sep=' ')
  dataNoGC = dataNoGC.drop(dataNoGC.index[112:140]).reset_index()
  start_time = dataNoGC.at[0, 'time']
  dataNoGC['time'] = dataNoGC['time'] - start_time
 
   
  x_NoGC = range(0, len(list(dataNoGC.index.values))) #dataNoGC.loc[:,'time']
  dram_used_NoGC = dataNoGC.loc[:,'dram_usedGB']

  fig, ax = plt.subplots(figsize=(15, 8))
  
  x= x_GC
  ax.plot(x_NoGC, dram_used_NoGC, label='No data liftime hints', color="#1f77b4", linewidth=4)
  ax.plot(x_GC, dram_used_GC, label='With data lifetime hints', color="#2ca02c", linewidth=4, linestyle='--')
  ax.set_xlim(0,180)
  ax.set_ylim(0,25)
  ax.set_xlabel("Time (s)")
  ax.set_ylabel("Capacity used (GB)")
  ax.legend(loc='upper left')
  #plt.show()
  plt.tight_layout()
  plt.savefig("gc_videoanalytics.pdf")

if __name__ == '__main__':
  noGC = sys.argv[1]
  withGC = sys.argv[2]
  plot_usage(noGC, withGC)
  
