import matplotlib
import matplotlib.pyplot as plt
import numpy as np

matplotlib.rcParams['pdf.fonttype'] = 42
matplotlib.rcParams['ps.fonttype'] = 42


cas_single_key_memory=('cas_single_key_memory', 2928908)
cas_single_key_device=('cas_single_key_device', 8974950) #I don't think I have this value

cas_multi_key_memory=('cas_multi_key_memory', 50559735.67)
cas_multi_key_device=('cas_multi_key_device', 91116408.54) #I don't think I have this value


labels = ["Single Address", "Independent Address"]
main_memory=[cas_single_key_memory[1],cas_multi_key_memory[1]]
device_memory=[cas_single_key_device[1],cas_multi_key_device[1]]

# labels = [cas_multi_qp_memory[0], cas_multi_qp_device[0], write_single_qp_memory[0], write_single_qp_dev[0]]
# mops = [cas_multi_qp_memory[1], cas_multi_qp_device[1], write_single_qp_memory[1], write_single_qp_dev[1]]

def div_mil(a):
    return[x/1000000.0 for x in a]

x = np.arange(len(labels))
print(x)

main_memory=div_mil(main_memory)
device_memory=div_mil(device_memory)

width=0.4
# plt.rcParams.update({'font.size': 16})
#fig, ax = plt.subplots()
factor=1.2
fig, ax = plt.subplots(1,1, figsize=(4,2.5))

ax.bar(x + width/2, device_memory, width, label="Device Memory", edgecolor='k')
ax.bar(x - width/2, main_memory, width, label="Main Memory", edgecolor='k')
ax.set_xticks(x,labels)

xalign=0.92

ax.text(xalign+(width/2)-1,10,str(round(device_memory[0]/main_memory[0],1))+"x")
ax.text(xalign+(width/2),10,str(round(device_memory[1]/main_memory[1],1))+"x")




# Add some text for labels, title and custom x-axis tick labels, etc.
ax.set_ylabel('MOPS')
#ax.set_xticks(x)
#ax.set_xticklabels(labels)
ax.legend()

fig.tight_layout()
figname="rdma_cas_throughput"
plt.savefig(figname+".pdf")
plt.savefig(figname+".png")