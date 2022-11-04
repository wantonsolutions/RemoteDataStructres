import matplotlib.pyplot as plt
import numpy as np

#values taken from row 8 of experiment
#002 bucket_vs_bound
#005 bucekt_vs_bound_bfs

labels = ['1', '2', '4', '8', '16', '32', '64']

fifty = [128,128,128,128,128,128,128]
ninety = [128,256,384,768,1536,3072,3840]
ninety_nine = [128,256,512,1024,2048,4096,6656]

x = np.arange(len(labels))  # the label locations
width = 0.35  # the width of the bars

fig, ax = plt.subplots()
ax.plot(labels, fifty, label='50th Percentile')
ax.plot(labels, ninety, label='90th Percentile')
ax.plot(labels, ninety_nine, label='99th Percentile')

ax.set_title('Bounded Cuckoo Insert Distance (bucket size 8)')
ax.set_xlabel('Bound Size')
ax.set_ylabel('insert distance (bytes)')

# Add some text for labels, title and custom x-axis tick labels, etc.
ax.set_xticks(x, labels)
ax.legend()

fig.tight_layout()

plt.savefig('insert_range_8.png')
plt.savefig('insert_range_8.pdf')
