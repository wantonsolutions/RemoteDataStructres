#!/bin/bash -x

packetsizes=( 1470 1500 2900 )
numinst=$1
time=10   
txintf=p5p2     # lo
rxintf=p5p1     # lo
output=tmp.txt
final=results.txt
cpusar=cpusar.dat
netsar=netsar.dat

exp_name=$2
exp_desc=$3
if [ -z "$exp_name" ] || [ -z "$exp_desc" ]; then
    echo "Provide a short name (folder name) and description for this run"
    exit -1
fi

link_mtu=$4
if [[ "$link_mtu" ]]; then
    sudo ifconfig $txintf mtu $link_mtu
    sudo ifconfig $rxintf mtu $link_mtu
    if [ $? -ne 0 ]; then
        echo "ERROR! Cannot set link mtu to $link_mtu"
        exit 1
    else
        echo "Set link mtu to $link_mtu"
    fi
fi

mkdir $exp_name
echo $exp_desc > $exp_name/readme

let 'sleeptime = time + 2'
let 'sartime = time - 2'

# Start iPerf server
./perf.sh -s -n=$numinst

echo "Instances","Buffer Size","TX Pkts","TX Mbps","RX Pkts","RX Mbps","Server CPU","Client CPU" \
    | tee ${exp_name}/results
# for size in ${packetsizes[@]}; do
for size in $(seq 50 500 20000); do

    # Start the clients
    ./perf.sh -c -n=$numinst -p=$size -t=$time &> /dev/null

    # Start SAR readings after a sec
    sleep 1
    sar -n DEV 1 $sartime > $netsar &
    sar -P ALL 1 $sartime > $cpusar &

    # Sleep until clients finish
    sleep $sleeptime

    # Parse results from sar output
    # These awk queries get cpu usage aggregated by even or odd cores as long as cores host iperf servers or clients  
    odd_cpu=$(cat $cpusar | awk '{ if( $1=="Average:" && $2!="all" && $2%2==1 && $2<2*'$numinst' ){ sum_cpu += ($3+$5); num_cpu += 1; }} END { if(num_cpu) print sum_cpu/num_cpu; else print 0; }')
    even_cpu=$(cat $cpusar | awk '{ if( $1=="Average:" && $2!="all" && $2%2==0 && $2<2*'$numinst' ){ sum_cpu += ($3+$5); num_cpu += 1; }} END { if(num_cpu) print sum_cpu/num_cpu; else print 0; }')
    
    avg_rx_mbps=$(cat $netsar | awk '/'$rxintf'/ { if( $1=="Average:" && $2=="'$rxintf'" ){ print $5*8/1000; } }')
    avg_tx_mbps=$(cat $netsar | awk '/'$txintf'/ { if( $1=="Average:" && $2=="'$txintf'"){ print $6*8/1000; } }')
    avg_rx_pkts=$(cat $netsar | awk '/'$rxintf'/ { if( $1=="Average:" && $2=="'$rxintf'"){ print $3; } }')
    avg_tx_pkts=$(cat $netsar | awk '/'$txintf'/ { if( $1=="Average:" && $2=="'$txintf'"){ print $4; } }')

    echo $numinst,$size,$avg_tx_pkts,$avg_tx_mbps,$avg_rx_pkts,$avg_rx_mbps,$even_cpu,$odd_cpu \
        | tee -a ${exp_name}/results

done

# Make a snapshot of the bash and plot scripts 
cp ./perfdriver.sh ${exp_name}/perfdriver.sh.snap
cp ./perf.sh ${exp_name}/perf.sh.snap
cp ./plot.py ${exp_name}/plot.py.snap

#python plot.py -d ${exp_name}/results -o ${exp_name}/${exp_name}.png -t "$exp_desc"