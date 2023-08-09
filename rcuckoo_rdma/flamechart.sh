
measuretime=40

# program=cuckoo_client
program=test/test_virtual_rdma
# program=test/test_search
sudo killall "$program"

# page_script="LD_PRELOAD=libhugetlbfs.so HUGETLB_MORECORE=yes"
export MLX5_SINGLE_THREADED=1
LD_PRELOAD=libhugetlbfs.so HUGETLB_MORECORE=yes ./"$program" &
# LD_PRELOAD=libhugetlbfs.so HUGETLB_MORECORE=yes ./test/test_search &
cuckoopid=$!


sudo perf record -F 99 -p$cuckoopid -g -- sleep $measuretime
sudo perf script | ./flamechart/FlameGraph/stackcollapse-perf.pl > out.perf-folded
sudo killall "$program"
./flamechart/FlameGraph/flamegraph.pl out.perf-folded > flamechart.svg
firefox flamechart.svg

