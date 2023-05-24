#!/bin/bash
# pip3 install numpy
# python3 -m pip install -U matplotlib
pip3 install .

pushd rcuckoo_rdma
./install.sh
popd
