pushd xxHash; make; sudo make install; popd
make clean; python setup.py build_ext --inplace;
pip3 install .
