import matplotlib.pyplot as plt

sherman_write_only = [0.299,0.538,1.047]
sherman_cores = [2,4,8]

fusee_write_only = [159270,287776,416964,552906,678810,809386,934386,1056780]
fusee_write_only = [x / 1000000 for x in fusee_write_only]
fusee_cores = [1,2,3,4,5,6,7,8]


rcuckoo_write_only=[120271.283245,234694.346647,459445.070634,917088.666479,1707337.275191,2391186.794957,]
rcuckoo_write_only = [x / 1000000 for x in rcuckoo_write_only]
rcuckoo_cores=[1,2,4,8,16,24]

clover_write_only=[357708,516197,617516,686380,735140,677488,538924,367619,548668,553904,]
clover_write_only = [x / 1000000 for x in clover_write_only]
clover_cores=[7,14,28,56,112,168,224,280,336,392,]

clover_write_only = clover_write_only[:3]
clover_cores = clover_cores[:3]



swordbox_write_only=[866916,1639656,3114720,5418308,9386624,12991886,14967071,17022698,17165892,17432716,]                                                                                                                     
swordbox_write_only = [x / 1000000 for x in swordbox_write_only]
swordbox_cores=[7,14,28,56,112,168,224,280,336,392,]

swordbox_write_only = swordbox_write_only[:3]
swordbox_cores = swordbox_cores[:3]






fig, ax = plt.subplots(1, 1, figsize=(5, 3))
ax.plot(sherman_cores,sherman_write_only, marker='x', label="sherman")
ax.plot(fusee_cores,fusee_write_only, marker='x', label="fusee")
ax.plot(rcuckoo_cores,rcuckoo_write_only, marker='x', label="rcuckoo")
ax.plot(clover_cores,clover_write_only, marker='x', label="clover (contention)")
ax.plot(swordbox_cores,swordbox_write_only, marker='x', label="swordbox (contention)")


ax.legend(ncol=2, prop={'size': 8})
ax.set_ylabel("MOPS")
ax.set_xlabel("Clients")
# ax.set_xlim(0,10)
# ax.set_xscale('log')
ax.set_title("Insert Only Throughput")
plt.tight_layout()
plt.savefig("hero_insert_only.pdf")