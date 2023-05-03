import sys
# caution: path[0] is reserved for script path (or '' in REPL)
sys.path.insert(1, '/home/ena/RemoteDataStructres/rmem_simulator')
import plot_cuckoo as pc
import data_management as dm
import matplotlib.pyplot as plt

print("import resolved")

plot_names = [
    "general_stats",
    "cas_success_rate",
    "read_write_ratio",
    "request_success_rate",
    "rtt_per_operation",
    "bytes_per_operation",
    "messages_per_operation",
    "fill_factor"
    ]

random_independent, _ = dm.load_statistics("random_independent")
random_dependent, _ = dm.load_statistics("random_dependent")
a_star_dependent, _ = dm.load_statistics("a_star_dependent")

percentiles=[50, 90, 99]
fig, axs = plt.subplots(len(percentiles), 1, figsize=(10, 12))

percentile=99
x_axis=pc.detect_x_axis(random_independent)
x_axis_vals = pc.get_x_axis(random_independent, x_axis)

for i in range(len(percentiles)):
    percentile=percentiles[i]
    ax = axs[i]
    random_independent_rtt, random_independent_err = pc.client_stats_get_percentile_err_trials(random_independent, 'insert_rtt', percentile)
    h1 = ax.errorbar(x_axis_vals,random_independent_rtt,yerr=random_independent_err, linestyle="-", marker="o", label="random independent")

    random_dependent_rtt, random_dependent_err = pc.client_stats_get_percentile_err_trials(random_dependent, 'insert_rtt', percentile)
    h1 = ax.errorbar(x_axis_vals,random_dependent_rtt,yerr=random_dependent_err, linestyle="-", marker="^", label="random (dependent)")

    a_star_dependent_rtt, a_star_dependent_err = pc.client_stats_get_percentile_err_trials(a_star_dependent, 'insert_rtt', percentile)
    h1 = ax.errorbar(x_axis_vals,a_star_dependent_rtt,yerr=a_star_dependent_err, linestyle="-", marker="s", label="a star (dependent)")

    race_mean=3
    ax.axhline(y=race_mean, color='r', linestyle=':', label="race mean (" +str(race_mean) + ")")
    ax.set_ylabel("RTT "+str(percentile)+"th percentile")
    ax.set_xlabel(x_axis)
    ax.set_ylim(bottom=0)
    ax.legend()

plt.tight_layout()
plt.savefig("rtt_microbenchmark.pdf")


# for s in stats:
#     pc.multi_plot_runs(s, plot_names)