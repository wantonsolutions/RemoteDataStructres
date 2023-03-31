import matplotlib.pyplot as plt
import numpy as np

def multi_plot_runs(runs, plot_names):
    print("plotting ", len(runs), " runs: with", len(plot_names), " plots" + str(plot_names))

    fig, axs = plt.subplots(len(plot_names), 1)
    if len(plot_names) == 1:
        axs = [axs]
    i=0
    for plot_name in plot_names:
        print(plot_name)
        if plot_name == "cas_success_rate":
            cas_success_rate(axs[i],runs)
        elif plot_name == "read_write_ratio":
            read_write_ratio(axs[i],runs)
        elif plot_name == "bytes_per_operation":
            bytes_per_operation(axs[i],runs)
        else:
            print("unknown plot name: ", plot_name)
        i+=1

    
    plt.tight_layout()
    plt.savefig("latest_multi_run.pdf")

def multi_plot_run(run, plot_names):
    print("plotting ", len(plot_names), " plots" + str(plot_names))

def stderr(data):
    return np.std(data) / np.sqrt(np.size(data))

def cas_success_rate(ax, stats):
    print("CAS SUCCESS RATE")
    print("todo - label graph with workload")
    print("todo print locking strategy")
    success_rates = []
    std_errs = []
    clients = []
    for stat in stats:
        per_client_success_rate = []
        for client in stat['clients']:
            total_cas = client['stats']['total_cas']
            total_cas_failure = client['stats']['total_cas_failures']
            success_rate = float(1.0 - float(total_cas_failure)/float(total_cas))
            per_client_success_rate.append(success_rate)

        success_rates.append(np.mean(per_client_success_rate))
        std_errs.append(stderr(per_client_success_rate))
        clients.append(str(len(stat['clients'])))

    x_pos = np.arange(len(success_rates))
    ax.bar(x_pos,success_rates,yerr=std_errs,align="center", edgecolor='black')
    ax.set_ylabel("CAS success rate")
    ax.set_xlabel("# clients")
    ax.set_xticks(x_pos)
    ax.set_xticklabels(clients)
    # ax.set_yscale('log')
    ax.set_title("CAS success rate vs # of clients")

def read_write_ratio(ax, stats):
    print("READ WRITE RATIO")
    print("TODO make the x axis configurable not just clients")

    #read write ratio should work for both single and multi run
    if isinstance(stats, dict):
        stats = [stats]

    read_rates=[]
    read_err=[]
    write_rates=[]
    write_err=[]
    clients = []
    for stat in stats:
        read_percent = []
        write_percent = []
        for client in stat['clients']:
            reads = client['stats']['completed_read_count']
            writes = client['stats']['completed_insert_count']
            total_ops = reads + writes
            read_percent.append(float(reads)/float(total_ops))
            write_percent.append(float(writes)/float(total_ops))
        read_rates.append(np.mean(read_percent))
        read_err.append(stderr(read_percent))
        write_rates.append(np.mean(write_percent))
        write_err.append(stderr(write_percent))
        clients.append(str(len(stat['clients'])))

    x_pos = np.arange(len(read_rates))
    bar_width = 0.35
    ax.bar(x_pos,read_rates,bar_width,yerr=read_err,align="center", edgecolor='black', label="Read")
    ax.bar(x_pos+bar_width,write_rates,bar_width,yerr=write_err,align="center", edgecolor='black', label="Write")
    ax.set_ylabel("Read/Write ratio")
    ax.set_xlabel("# clients")
    ax.set_xticks(x_pos+bar_width/2)
    ax.set_xticklabels(clients)
    ax.legend()

def bytes_per_operation(ax, stats):
    print("BYTES PER OPERATION")
    print("TODO make the x axis configurable not just clients")
    print("TODO make the y axis configurable not just bytes")
    print("TODO this plot makes the assumption that read make up all of the read operation")
    print("TODO this plot makes the assumption that CAS makes up all of the write operations")

    #read write ratio should work for both single and multi run
    if isinstance(stats, dict):
        stats = [stats]

    read_bytes=[]
    read_err=[]
    write_bytes=[]
    write_err=[]
    clients = []
    for stat in stats:
        read_bytes_per_op = []
        write_bytes_per_op = []
        for client in stat['clients']:
            reads = client['stats']['completed_read_count']
            writes = client['stats']['completed_insert_count']
            read_bytes_per_op.append(float(client['stats']['read_bytes'])/float(reads))
            write_bytes_per_op.append(float(client['stats']['cas_bytes'])/float(writes))
        read_bytes.append(np.mean(read_bytes_per_op))
        read_err.append(stderr(read_bytes_per_op))
        write_bytes.append(np.mean(write_bytes_per_op))
        write_err.append(stderr(write_bytes_per_op))
        clients.append(str(len(stat['clients'])))

    x_pos = np.arange(len(read_bytes))
    bar_width = 0.35
    ax.bar(x_pos,read_bytes,bar_width,yerr=read_err,align="center", edgecolor='black', label="Read")
    ax.bar(x_pos+bar_width,write_bytes,bar_width,yerr=write_err,align="center", edgecolor='black', label="Write")
    ax.legend()
    ax.set_yscale('log')
    ax.set_ylabel("Bytes per operation")
    ax.set_xlabel("# clients")
    ax.set_xticks(x_pos+bar_width/2)
    ax.set_xticklabels(clients)



        



