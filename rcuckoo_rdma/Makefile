# location of the Python header files
# PYTHON_VERSION = 3.8
# PYTHON_VERSIONNS = 38
# PYTHON_INCLUDE = /usr/include/python$(PYTHON_VERSION)
# location of the Boost Python include files and library
# BOOST_INC = /usr/local/home/ssgrant/boost_1_82_0/include
# BOOST_LIB = /usr/local/home/ssgrant/boost_1_82_0/lib
XXHASH_INC = /usr/local/home/ssgrant/RemoteDataStructres/rcuckoo_rdma/xxHash

CPP_FLAGS = -DFAST
# CPP_FLAGS += -DDEBUG
# compile mesh classes

TARGET = hashpp
$(TARGET).so: $(TARGET).o
	g++ -shared -Wl,--export-dynamic $(TARGET).o -L$(XXHASH_INC) -lxxhash -L$(BOOST_LIB) -lboost_python$(PYTHON_VERSIONNS) -o $(TARGET).so -lssl -lcrypto 
$(TARGET).o: $(TARGET).cpp
	g++ -I$(PYTHON_INCLUDE) -I$(BOOST_INC)  -I$(XXHASH_INC) $(CPP_FLAGS) -fPIC -c $(TARGET).cpp

clean:
	rm -f *.o *.so
	rm chash.cpp
	rm -r build