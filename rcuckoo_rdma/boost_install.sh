#install the boost library
pushd /usr/local/home/ssgrant

if [ ! -d "boost_1_82_0" ]; then
    echo "boost_1_82_0 does not exist"
    wget https://boostorg.jfrog.io/artifactory/main/release/1.82.0/source/boost_1_82_0.tar.bz2
    tar --bzip2 -xf ./boost_1_82_0.tar.bz2
    rm boost_1_82_0.tar.bz2
else
    echo "boost_1_82_0 exists continuing"
fi

cd boost_1_82_0
./bootstrap.sh --prefix=/usr/local/home/ssgrant/boost_1_82_0
./b2 install
./bjam install

popd