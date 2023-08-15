import numpy as np
import matplotlib.pyplot as plt

from experiments import data_management as dm

def cdf(data):
    high = max(data)
    low = min(data)
    # norm = plt.colors.Normalize(low,high)

    #print(data)
    count, bins_count = np.histogram(data, bins = 100000 )
    pdf = count / sum(count)
    
    y = np.cumsum(pdf)
    x = bins_count[1:]

    y= np.insert(y,0,0)
    x= np.insert(x,0,x[0])
    return x, y


dm.load_statistics("FUSEE_Latecy")


x, y = cdf(insert_data)
ax.plot(x,y, label="insert")
x, y = cdf(read_data)
ax.plot(x,y, label="read")
x, y = cdf(update_data)
ax.plot(x,y, label="update")

ax.legend()
ax.set_xlim(left=1)
ax.set_xscale('log')
ax.set_ylabel("CDF")
ax.set_xlabel("Latency (us)")
ax.set_title("FUSEE operation latency")
plt.tight_layout()

plt.savefig("FUSEE-initial-latency.pdf")