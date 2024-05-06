import sys

from experiments import plot_cuckoo as pc
from experiments import data_management as dm
import matplotlib.pyplot as plt
import lib

from simulator import hash
# import chash as hash

# factors = [1.8, 1.9, 2.0, 2.1, 2.2, 2.3]
factors = [2.1, 2.3, 2.5]
data_dir="hash_factor"

def gen_hash_factor_distance_cdf():
    global factors

    samples = 100000
    table_size = 512
    bucket_size = 8
    all_distances=[]
    for f in factors:
        print("factor: " + str(f))
        hash.set_factor(f)
        distances = []
        for i in range(samples):
            v1, v2 = hash.rcuckoo_hash_locations(i, table_size)
            distances.append(hash.distance_to_bytes(v1, v2, bucket_size, 8))
        print("len distances:", len(distances))
        all_distances.append(distances)
    dm.save_statistics(all_distances,dirname=data_dir)
    print("all distance prior to save: ", len(all_distances))


def plot_hash_factor_distance_cdf():
    global factors
    fig, ax = plt.subplots(1,1,figsize=(4,2.5))
    all_distances = dm.load_statistics(dirname=data_dir)
    all_distances = all_distances[0]
    print("all distance post load: ", len(all_distances))
    
    colors=[lib.fusee_color,lib.rcuckoo_color,lib.sherman_color]
    for i in range(len(factors)):
        distances = all_distances[i]
        distances = [(v/64) for v in distances]
        print(distances)
        f = factors[i]
        x, y = pc.cdf(distances)
        ax.plot(x,y, label="f="+str(f), color=colors[i])

    # x = [1,2,4,8,16,32,64, 128, 256, 512, 1024, 2048, 4096] 
    # x = [2,8,32,128,512,2048]
    x = [1,4,16,64,256,1024]
    # rows = [ int(v / 32) for v in x]
    x_str = [str(v) for v in x]
    # x_str = ["32", "64", "128", "256", "512", "1K", "2K", "4K"]
    # x_str = [ str(i) for i in x]

    ax.set_xlabel('distance (table rows)')
    ax.set_ylabel('CDF')
    # ax.set_title('Exponetial Hash Factor Distance')
    ax.set_xlim(0.5,1000)
    ax.set_ylim(0,1.01)
    ax.set_xscale('log')
    ax.minorticks_off()

    ax.set_xticks(x)
    ax.set_xticklabels(x_str)
    ax.legend()
    plt.tight_layout()
    plt.savefig("hash_factor.pdf")

# gen_hash_factor_distance_cdf()
plot_hash_factor_distance_cdf()