#!/bin/bash
# pyflame="/home/ena/RemoteDataStructres/profiling/pyflame/src/pyflame"
# flamechart="~/RemoteDataStructures/profiling/FlameGraph/flamegraph.pl"
# python3 run_experiments.py &
# pid="$!"
# sudo "$pyflame" -p $pid

# test_file=run_experiments.py
# test_file=test_lock_table_perf.py
test_file=simulation_runtime.py

# python3 -O -m cProfile -o profile.prof $test_file
python3 -O -m cProfile -o profile.prof -m simulator.simulation_runtime
flameprof profile.prof > profile.svg



# sudo killall python3