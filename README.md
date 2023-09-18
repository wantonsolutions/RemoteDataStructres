# Remote Data Structures

This repository is for designing and implementing data structures for remote memory.


# Installation notes
circa Aug 16 2023 this is what I'm using to install and some tips.

I'm staging all of my installations at 
__/usr/local/home/ssgrant__

### MLNX_OFED version
__MLNX_OFED_LINUX-4.9-6.0.6.0__

### ofed kernel
5.4.0-48-generic

#### list the installed kernels
awk -F\' '/menuentry / {print $2}' /boot/grub/grub.cfg
sudo vim /etc/default/grum
>>edit line GRUB_DEFAULT to the entry you need
sudo update-grub

I'm using yak01 as a build server so most data can be acquired by rsyncing with it.
rsync -r yak-01.sysnet.ucsd.edu:/usr/local/home/ssgrant/

## FUSEE

sudo apt-get install libhugetlbfs-bin
sudo apt install libtbb-dev
sudo apt-get install libgtest-dev
sudo apt-get install libboost-all-dev

### set hugepages
sudo hugeadm --pool-pages-min 2MB:65536
sudo apt-get install libmemcached-dev


## Sherman
pushd /usr/local/home/ssgrant/RemoteDataStructres/systems/Sherman/cityhash ; sudo make install; popd




