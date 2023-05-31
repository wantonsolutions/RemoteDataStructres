import sys
import os
import re
import math
from datetime import datetime
from datetime import timedelta
import time
import random
import matplotlib.pyplot as plt
from collections import Counter, defaultdict
import argparse
import pandas as pd


colors = ['orange','orangered','brown','r','c','k', 'm', 'g']
linetypes = ['g-','g--','g-+']
markers = ['x','^','s','o','+','*', '|']


def main():
    parser = argparse.ArgumentParser("Plots network throughput and cpu usage")
    parser.add_argument('-d', '--datafile', action='store', help='path to the data file', required=True)
    parser.add_argument('-o', '--output', action='store', help='path to the output plot png', default="result.png")
    parser.add_argument('-p', '--print_', action='store_true', help='print data (with nicer format) instead of plot', default=False)
    parser.add_argument('-t', '--plottitle', action='store', help='title of the plot')
    args = parser.parse_args()

    if not os.path.exists(args.datafile):
        return -1
    
    df = pd.read_csv(args.datafile)
    if args.print_:
        print(df)
        return 0

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12,6))
    # fig.set_size_inches(w=,h=6)
    fig.suptitle(args.plottitle)
    # ax1.set_xlabel("Buffer Size (B)")
    ax1.set_ylabel("Xput Mbps")
    ax1.plot(df["Buffer Size"], df["RX Mbps"], label="RX Mbps",color='orange',marker='x',markerfacecolor='none')
    ax1.plot(df["Buffer Size"], df["TX Mbps"], label="TX Mbps",color='blue',marker='x',markerfacecolor='none')
    ax1.legend(ncol=1,loc="upper left")
    # ax1.set_ylim([0, 30000])
    
    ax1t = ax1.twinx()
    ax1t.set_ylabel("Kilo Pkts Per Sec")
    ax1t.plot(df["Buffer Size"], df["TX Pkts"]/1000, label="TX Pkts",color='orangered',marker='o',markerfacecolor='none')
    ax1t.plot(df["Buffer Size"], df["RX Pkts"]/1000, label="RX Pkts",color='green',marker='o',markerfacecolor='none')
    ax1t.legend(ncol=1,loc="lower right")
    # ax1t.set_ylim([0, 10000])

    # ax2 = ax1.twinx()
    ax2.set_xlabel("Buffer Size (B)")
    ax2.set_ylabel("CPU Usage")
    # ax2.plot(df["Buffer Size"], df["CPU"], label="CPU",color='blue',marker='x',markerfacecolor='none')
    ax2.plot(df["Buffer Size"], df["Server CPU"], label="Server CPU",color='blue',marker='x',markerfacecolor='none')
    ax2.plot(df["Buffer Size"], df["Client CPU"], label="Client CPU",color='orange',marker='x',markerfacecolor='none')
    ax2.legend(ncol=1,loc="lower right")
    ax2.set_ylim([0, 100])
    
    plt.savefig(args.output)
    plt.show()


if __name__ == '__main__':
    main()