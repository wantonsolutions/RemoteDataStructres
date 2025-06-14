import lib

import experiments.plot_cuckoo as plot_cuckoo
import experiments.data_management as dm

import simulator.log as log
import simulator.state_machines as sm
import simulator.simulation_runtime as sim
import simulator.cuckoo as cuckoo

import matplotlib.pyplot as plt
import matplotlib as matplotlib
import numpy as np
from tqdm import tqdm

import matplotlib
matplotlib.rcParams['pdf.fonttype'] = 42
matplotlib.rcParams['ps.fonttype'] = 42


data_dir = "buckets_per_lock_vs_locks_per_message"
def buckets_per_lock_vs_locks_per_message_experiment():
    logger = log.setup_custom_logger('root')
    logger.info("Starting simulator")

    table_size = 4096
    clients=8
    all_runs=[]
    read_threshold=128
    buckets_per_lock_arr=[1, 2, 4, 8, 16, 32, 64, 128, 256, 512]
    locks_per_message=[1,2,4,8,16,32,64]
    log.set_off()
    # log.set_debug()
    # locks_per_message_arr=[1, 2]
    for lpm in locks_per_message:
        runs=[]
        for buckets_per_lock in buckets_per_lock_arr:
            config = lib.get_config()
            config['indexes'] = table_size
            config['num_clients'] = clients
            config['num_steps'] = 1000000
            config['read_threshold_bytes'] = read_threshold

            config['bucket_size']=8
            config["buckets_per_lock"] = buckets_per_lock
            config["locks_per_message"] = lpm
            config["state_machine"] = cuckoo.rcuckoo
            config["max_fill"] = 100
            runs.append(sim.run_trials(config))
        all_runs.append(runs)
    dm.save_statistics(all_runs,dirname=data_dir)

def plot_buckets_per_lock_vs_locks_per_message_experiment():
    stats = dm.load_statistics(dirname=data_dir)
    # fig, ax = plt.subplots()

    rtt_matrix=[]
    stats = stats[0]
    for stat in stats:
        rtt_row=[]
        for s_full in stat:
            s = s_full[0]
            print(s)
            config=s['config']
            buckets_per_lock=config['buckets_per_lock']
            locks_per_message=config['locks_per_message']

            percentile = 99
            insert_rtt, insert_err = plot_cuckoo.client_stats_get_percentile_err(s, 'insert_rtt', percentile)

            # print("buckets per lock: ", buckets_per_lock, " locks per message: ", locks_per_message, " fill: ", fill)
            rtt_row.append((buckets_per_lock, locks_per_message,insert_rtt))
        rtt_matrix.append(rtt_row)
    print(rtt_matrix)

    x_axis_buckets_per_lock = []
    for i in range(len(rtt_matrix[0])):
        x_axis_buckets_per_lock.append(rtt_matrix[0][i][0])
    print(x_axis_buckets_per_lock)

    y_axis_locks_per_message = []
    for i in range(len(rtt_matrix)):
        y_axis_locks_per_message.append(rtt_matrix[i][0][1])
    print(y_axis_locks_per_message)

    z_axis_rtt = []
    for i in range(len(rtt_matrix)):
        row=[]
        for j in range(len(rtt_matrix[0])):
            row.append(rtt_matrix[i][j][2])
        z_axis_rtt.append(row)
    print(z_axis_rtt)



    # ax.imshow(z_axis_rtt, cmap='summer', interpolation='nearest')
    # for i in range(len(y_axis_locks_per_message)):
    #     for j in range(len(x_axis_buckets_per_lock)):
    #         v = int(round(z_axis_rtt[i][j],1))
    #         text = ax.text(j, i, v,
    #                     ha="center", va="center", color="w")

    factor = 1.5
    fig, ax = plt.subplots(1,1, figsize=(3*factor,2*factor))

    z_axis_rtt = np.array(z_axis_rtt)
    im, cbar = lib.heatmap(z_axis_rtt, y_axis_locks_per_message, x_axis_buckets_per_lock, ax=ax,
                    cmap="YlGn_r", cbarlabel="Lock Aquire RTT")
    texts = lib.annotate_heatmap(im, valfmt="{x:.0f}")

    # ax.colorbar()
    # plt.yticks(np.arange(len(y_axis_locks_per_message)), y_axis_locks_per_message)
    # plt.xticks(np.arange(len(x_axis_buckets_per_lock)), x_axis_buckets_per_lock)
    ax.set_xlabel("Rows per lock")
    ax.set_ylabel("Locks per message")
    fig.tight_layout()
    plt.tight_layout()
    plt.savefig("buckets_per_lock_vs_locks_per_message.pdf")

# buckets_per_lock_vs_locks_per_message_experiment()
plot_buckets_per_lock_vs_locks_per_message_experiment()