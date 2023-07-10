
measuretime=30

sudo killall test_cuckoo

# page_script="LD_PRELOAD=libhugetlbfs.so HUGETLB_MORECORE=yes"
LD_PRELOAD=libhugetlbfs.so HUGETLB_MORECORE=yes ./test/test_cuckoo &
cuckoopid=$!


sudo perf record -F 99 -p$cuckoopid -g -- sleep $measuretime
sudo perf script | ./flamechart/FlameGraph/stackcollapse-perf.pl > out.perf-folded
sudo killall test_cuckoo
./flamechart/FlameGraph/flamegraph.pl out.perf-folded > flamechart.svg
firefox flamechart.svg

