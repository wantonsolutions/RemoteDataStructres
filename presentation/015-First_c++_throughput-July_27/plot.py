import matplotlib.pyplot as plt
import numpy as np


tput=[94552,179834,332862,730808,1251195 ,1646772]
clients=[1,2,4,8,16,24]
fig, ax = plt.subplots(1, 1, figsize=(5, 3))

for i in range(len(clients)):
    tput[i]=tput[i]/1000000.0

ax.plot(clients,tput, marker='o')
ax.set_ylabel("MOPS")
ax.set_xlabel("Clients")
ax.set_title("Throughput vs Clients: Fill to 10%, 100% writes")
ax.set_ylim(0)
ax.set_xlim(0)

plt.tight_layout()
plt.savefig('throughput.pdf')