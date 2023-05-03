import sys
# caution: path[0] is reserved for script path (or '' in REPL)
sys.path.insert(1, '/home/ena/RemoteDataStructres/rmem_simulator')
import plot_cuckoo as pc
import data_management as dm
import matplotlib.pyplot as plt
import hash

# factors = [1.8, 1.9, 2.0, 2.1, 2.2, 2.3]
factors = [1.9, 2.1, 2.3]
data_dir="hash_factor"

def gen_hash_factor_distance_cdf():
    global factors

    samples = 10000
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
    fig, ax = plt.subplots(1,1,figsize=(5,2.5))
    all_distances = dm.load_statistics(dirname=data_dir)
    all_distances = all_distances[0]
    print("all distance post load: ", len(all_distances))

    for i in range(len(factors)):
        distances = all_distances[i]
        f = factors[i]
        x, y = pc.cdf(distances)
        ax.plot(x,y, label="f="+str(f))

    x = [32, 64, 128, 256, 512, 1024, 2048, 4096]
    x_str = [ str(i) for i in x]

    ax.set_xlabel('Distance (Bytes)')
    ax.set_ylabel('CDF')
    # ax.set_title('Exponetial Hash Factor Distance')
    ax.set_xlim(32,5000)
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