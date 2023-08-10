

program=cuckoo_client

# page_script="LD_PRELOAD=libhugetlbfs.so HUGETLB_MORECORE=yes"
export MLX5_SINGLE_THREADED=1
LD_PRELOAD=libhugetlbfs.so HUGETLB_MORECORE=yes \
numactl --cpunodebind=0 --membind=0 ./"$program"

