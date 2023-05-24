#!/bin/bash
# pip3 install numpy
# python3 -m pip install -U matplotlib

git submodule update --init --recursive

pip3 install Cython
pip3 install .

pushd rcuckoo_rdma
./install.sh
popd
