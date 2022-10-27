import matplotlib.pyplot as plt
import numpy as np

#values taken from row 8 of experiment
#002 bucket_vs_bound
#005 bucekt_vs_bound_bfs

labels = ['1', '2', '4', '8', '16']
bfs_mean = [0.62,0.81,0.9, 0.93, 0.95]
dfs_random_mean = [0.53,0.7,0.76,0.85,0.94]

x = np.arange(len(labels))  # the label locations
width = 0.35  # the width of the bars

fig, ax = plt.subplots()
rects2 = ax.bar(x - width/2, dfs_random_mean, width, label='Random')
rects1 = ax.bar(x + width/2, bfs_mean, width, label='BFS')

# Add some text for labels, title and custom x-axis tick labels, etc.
ax.set_ylabel('50th Percentile Fill (before cycle)')
ax.set_xlabel('Bound Size')
ax.set_title('Bounded Cuckoo Fill (bucket size 8)- BFS vs Random')
ax.set_xticks(x, labels)
ax.legend()

ax.bar_label(rects1, padding=3)
ax.bar_label(rects2, padding=3)

fig.tight_layout()

plt.savefig('random_vs_bfs_fill_8.png')
plt.savefig('random_vs_bfs_fill_8.pdf')
