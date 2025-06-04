import matplotlib.pyplot as plt

import matplotlib
matplotlib.rcParams['pdf.fonttype'] = 42
matplotlib.rcParams['ps.fonttype'] = 42

message_size = [2,4,8,16,32,64,128,256,512,"1K"]
latency = [0.8, 0.8, 0.8, 0.81, 0.83, 0.85, 0.9, 1.1, 1.25, 1.4]

# fig, ax = plt.subplots(1,1,figsize=(4,2))
fig, ax = plt.subplots(1,1, figsize=(4,2.5))

x_ticks = [str(x) for x in message_size]

ax.plot(x_ticks, latency, marker='o')
ax.set_xlabel("Message Size (bytes)")
ax.set_ylabel("Latency (us)")
ax.set_ylim(bottom=0, top=1.5)
plt.grid(axis='y')
plt.tight_layout()
plt.savefig("rdma_latency.pdf")
