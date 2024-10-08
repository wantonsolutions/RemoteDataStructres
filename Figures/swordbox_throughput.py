import matplotlib
import matplotlib.pyplot as plt
from statistics import mean
import numpy as np
from common import *

plt.rcParams.update({'font.size': 16})
default_line_width=3
default_marker_size=10

fig, (ax1, ax2, ax3) = plt.subplots(1,3, figsize=(15,4))

def tput_err(ax, rws, ws, clover, show_improvement=True):
    if rws != None:
        scale_mil(rws)
        ax.errorbar(rws["threads"],rws["ops"],rws["err"],label=read_write_steering_label, marker=read_write_steering_marker, color=read_write_steering_color, linewidth=default_line_width, markersize=default_marker_size)

    if clover != None:
        scale_mil(clover)
        ax.errorbar(clover["threads"],clover["ops"],clover["err"],label=default_clover_label,marker=default_clover_marker, color=default_clover_color,linewidth=default_line_width, markersize=default_marker_size )
    if ws != None:
        scale_mil(ws)
        ax.errorbar(ws["threads"],ws["ops"],ws["err"],label=write_steering_label,marker=write_steering_marker,color=write_steering_color,linewidth=default_line_width, markersize=default_marker_size )

    if show_improvement:
        plot_max_improvement(ax,rws,clover,"threads")

    #ax.set_yscale("log")
    #ax.set_xlabel('Threads')



####################### YCSB C
avg_ops=[1829774,3577880,7008356,13684405,25656091,34720826,39142324,39713784,39970952,40600024,]
threads=[7,14,28,56,112,168,224,280,336,392,]
std=[0,0,0,0,0,0,0,0,0,0,]
clover_with_buffering_C= {"ops": avg_ops,"threads": threads, "err": std}
avg_ops=[1836632,3572209,7003510,13673092,25654763,34857976,39180515,39757292,39917704,40295512,]
threads=[7,14,28,56,112,168,224,280,336,392,]
std=[0,0,0,0,0,0,0,0,0,0,]
write_steering_C=        {"ops": avg_ops,"threads": threads, "err": std}
avg_ops=[1832205,3567680,7009575,13703432,25692824,34795712,39152679,39790996,39960749,40455924,]
threads=[7,14,28,56,112,168,224,280,336,392,]
std=[0,0,0,0,0,0,0,0,0,0,]
read_write_steering_C=   {"ops": avg_ops,"threads": threads, "err": std}


####################### YCSB B
avg_ops=[1487724,2655252,4818364,7666020,12365564,13979169,13716786,13427863,12837368,12382960,]
threads=[7,14,28,56,112,168,224,280,336,392,]
std=[0,0,0,0,0,0,0,0,0,0,]
clover_with_buffering_B= {"ops": avg_ops,"threads": threads, "err": std}
avg_ops=[1500840,2680607,4929508,7935485,12774234,14548308,14439633,14124372,13402903,13402903,]
threads=[7,14,28,56,112,168,224,280,336,392,]
std=[0,0,0,0,0,0,0,0,0,0,]
write_steering_B=        {"ops": avg_ops,"threads": threads, "err": std}

avg_ops=[1748703,3377649,6438944,12586564,21354096,28715113,34991652,34930195,35529061,35252996,]
threads=[7,14,28,56,112,168,224,280,336,392,]
std=[0,0,0,0,0,0,0,0,0,0,]
read_write_steering_B=   {"ops": avg_ops,"threads": threads, "err": std}



####################### YCSB A
avg_ops=[554788,877068,1120964,1312144,1509604,1450528,1226730,870612,1180566,1257288,]
threads=[7,14,28,56,112,168,224,280,336,392,]
std=[0,0,0,0,0,0,0,0,0,0,]
clover_with_buffering_A={"ops": avg_ops,"threads": threads, "err": std}
avg_ops=[815460,1331156,1858488,2343504,2639795,2386740,2098574,1471908,1830812,1602349,]
threads=[7,14,28,56,112,168,224,280,336,392,]
std=[0,0,0,0,0,0,0,0,0,0,]
write_steering_A={"ops": avg_ops,"threads": threads, "err": std}
avg_ops=[1194607,2264604,4147258,8024861,13868472,19604656,24528677,28124268,28348060,28201180,]
threads=[7,14,28,56,112,168,224,280,336,392,]
std=[0,0,0,0,0,0,0,0,0,0,]
read_write_steering_A={"ops": avg_ops,"threads": threads, "err": std}

clover_only = False
write_clover = False
read_clover = True

if clover_only == True:
    tput_err(ax1,None,None,clover_with_buffering_C, show_improvement=False)
    ax1.set_ylim(0,42)
    ax1.set_title('0% Writes')
    ax1.set_ylabel('MOPS')
    ax1.legend(loc='lower right', ncol=1)

    ax2.set_ylim(0,37)
    tput_err(ax2,None,None,clover_with_buffering_B, show_improvement=False)
    ax2.set_title('5% Writes')

    ax3.set_ylim(0,31)
    tput_err(ax3,None,None,clover_with_buffering_A, show_improvement=False)
    ax3.set_title('50% Writes')

    fig.text(0.515,0.00, "Threads", ha='center', size=18)
    plt.tight_layout()
    plt.savefig("full_system_00.png")
    plt.savefig("full_system_00.pdf")

elif write_clover == True:
    tput_err(ax1,None,write_steering_C,clover_with_buffering_C, show_improvement=False)
    ax1.set_ylim(0,42)
    ax1.set_title('0% Writes')
    ax1.set_ylabel('MOPS')
    ax1.legend(loc='lower right', ncol=1)

    ax2.set_ylim(0,37)
    tput_err(ax2,None,write_steering_B,clover_with_buffering_B, show_improvement=False)
    ax2.set_title('5% Writes')

    ax3.set_ylim(0,31)
    tput_err(ax3,None,write_steering_A,clover_with_buffering_A, show_improvement=False)
    ax3.set_title('50% Writes')

    fig.text(0.515,0.00, "Threads", ha='center', size=18)
    plt.tight_layout()
    plt.savefig("full_system_01.png")
    plt.savefig("full_system_01.pdf")
elif read_clover == True:
    tput_err(ax1,read_write_steering_C,None,clover_with_buffering_C, show_improvement=False)
    ax1.set_ylim(0,42)
    ax1.set_title('0% Writes')
    ax1.set_ylabel('MOPS')
    ax1.legend(loc='lower right', ncol=1)

    ax2.set_ylim(0,37)
    tput_err(ax2,read_write_steering_B,None,clover_with_buffering_B, show_improvement=True)
    ax2.set_title('5% Writes')

    ax3.set_ylim(0,31)
    tput_err(ax3,read_write_steering_A,None,clover_with_buffering_A, show_improvement=True)
    ax3.set_title('50% Writes')

    fig.text(0.515,0.00, "Threads", ha='center', size=18)
    plt.tight_layout()
    plt.savefig("swordbox.png")
    plt.savefig("swordbox.pdf")
else:
    tput_err(ax1,read_write_steering_C,write_steering_C,clover_with_buffering_C, show_improvement=False)
    ax1.set_ylim(0,42)
    ax1.set_title('0% Writes')
    ax1.set_ylabel('MOPS')
    ax1.legend(loc='lower right', ncol=1)

    ax2.set_ylim(0,37)
    tput_err(ax2,read_write_steering_B,write_steering_B,clover_with_buffering_B)
    ax2.set_title('5% Writes')

    ax3.set_ylim(0,31)
    tput_err(ax3,read_write_steering_A,write_steering_A,clover_with_buffering_A)
    ax3.set_title('50% Writes')

    fig.text(0.515,0.00, "Threads", ha='center', size=18)
    plt.tight_layout()
    plt.savefig("full_system_02.png")
    plt.savefig("full_system_02.pdf")

