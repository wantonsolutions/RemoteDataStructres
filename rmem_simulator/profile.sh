#!/bin/bash
# pyflame="/home/ena/RemoteDataStructres/profiling/pyflame/src/pyflame"
# flamechart="~/RemoteDataStructures/profiling/FlameGraph/flamegraph.pl"
# python3 run_experiments.py &
# pid="$!"
# sudo "$pyflame" -p $pid

python3 -m cProfile -o profile.prof run_experiments.py
flameprof profile.prof > profile.svg



# sudo killall python3