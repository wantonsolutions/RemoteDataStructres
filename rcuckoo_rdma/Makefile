
CC := g++ # define the C/C++ compiler to use
XXHASH_INC := /usr/local/home/ssgrant/RemoteDataStructres/rcuckoo_rdma/xxHash
MLX5_INC := /usr/include/infiniband
INCLUDES := -I$(XXHASH_INC) -I$(MLX5_INC)

# CFLAGS := -std=c++2a -g -O3 -Wall -Wall -Wextra -pedantic -fPIC

# CFLAGS := -std=c++2a -g -O3 -Wall -Wall -Wextra -pedantic -fPIC

# DEFINE_MACROS := -DVALUE_SIZE=12
DEFINE_MACROS := -DNOTHING
ifdef VALUE_SIZE
	DEFINE_MACROS := -DVALUE_SIZE=$(VALUE_SIZE)
endif
# -DVALUE_SIZE=28
CFLAGS := -std=c++2a -g -Wall -Wall -Wextra -pedantic -fPIC  $(DEFINE_MACROS)
# CFLAGS := -s -std=c++2a -O3 -Ofast -Wall -Wall -Wextra -pedantic -DNDEBUG -march=native
# CFLAGS := -std=c++2a -g -Wall -Wall -Wextra -pedantic -fPIC
# CFLAGS := -std=c++2a -g  -Wall -Wall -Wextra -pedantic -fPIC
# CFLAGS := -std=c++2a -O3 -Wall -Wall -Wextra -pedantic -fPIC -DNDEBUG

RDMA_LIBS := -lrdmacm -libverbs -lpthread

LIBRARIES := -L$(XXHASH_INC) -lxxhash $(RDMA_LIBS) -lmemcached -lssl -lcrypto -ljsoncpp

# TARGET := test_hash test_tables test_search rdma_server rdma_client


# TARGET := test/test_hash test_tables test_search test_state_machines test_virtual_rdma  test_cuckoo
test_dir := ./test
TARGET := $(test_dir)/test_hash \
	$(test_dir)/test_tables \
	$(test_dir)/test_search \
	$(test_dir)/test_state_machines \
	$(test_dir)/test_virtual_rdma \
	$(test_dir)/test_cuckoo rdma_memory_server \
	$(test_dir)/test_memcached \
	hash.o \
	tables.o \
	search.o \
	util.o \
	virtual_rdma.o \
	state_machines.o \
	cuckoo.o \
	rdma_engine.o \
	rdma_common.o \
	rdma_client_lib.o \
	rdma_client.o \
	rdma_server.o \
	rdma_helper.o \
	rdma_memory_server.o \
	memcached.o \
	cuckoo_client
# TARGET := rdma_memory_server
# TARGET := rdma_server
# TARGET := rdma_server rdma_client

all: $(TARGET)

hash.o : hash.cpp hash.h
	g++ -c $(CFLAGS) $(INCLUDES) -fPIC hash.cpp -o hash.o 

tables.o: tables.cpp tables.h
	g++ -c $(CFLAGS) $(INCLUDES) -fPIC tables.cpp -o tables.o

search.o: search.cpp search.h tables.o hash.o
	g++ -c $(CFLAGS) $(INCLUDES) -fPIC search.cpp -o search.o tables.o hash.o

util.o:	util.cpp util.h
	g++ -c $(CFLAGS) $(INCLUDES) -fPIC util.cpp -o util.o

virtual_rdma.o: virtual_rdma.cpp virtual_rdma.h util.o
	g++ -c $(CFLAGS) $(INCLUDES) -fPIC virtual_rdma.cpp -o virtual_rdma.o util.o

rdma_helper.o: rdma_helper.cpp rdma_helper.h
	$(CC) -c $(CFLAGS) $(INCLUDES) -fPIC rdma_helper.cpp -o rdma_helper.o $(RDMA_LIBS)

state_machines.o: state_machines.cpp state_machines.h tables.o hash.o search.o virtual_rdma.o util.o
	g++ -c $(CFLAGS) $(INCLUDES) -fPIC state_machines.cpp -o state_machines.o virtual_rdma.o util.o

cuckoo.o: cuckoo.cpp cuckoo.h state_machines.o tables.o hash.o search.o virtual_rdma.o rdma_engine.o
	g++ -c $(CFLAGS) $(INCLUDES) -fPIC cuckoo.cpp -o cuckoo.o state_machines.o tables.o hash.o search.o virtual_rdma.o rdma_engine.o $(RDMA_LIBS)

config.o: config.cpp config.h
	g++ -c $(CFLAGS) $(INCLUDES) -fPIC config.cpp -o config.o $(LIBRARIES)

rdma_engine.o: rdma_engine.cpp rdma_engine.h state_machines.o virtual_rdma.o rdma_client_lib.o rdma_common.o memcached.o config.h rdma_helper.o
	g++ -c $(CFLAGS) $(INCLUDES) -fPIC rdma_engine.cpp -o rdma_engine.o state_machines.o virtual_rdma.o rdma_common.o rdma_client_lib.o memcached.o rdma_helper.o config.o $(RDMA_LIBS) $(LIBRARIES)

rdma_common.o: rdma_common.cpp
	$(CC) -c $(CFLAGS) $(INCLUDES) -fPIC rdma_common.cpp -o rdma_common.o


rdma_client_lib.o: rdma_common.o rdma_client_lib.cpp rdma_client_lib.h
	$(CC) -c $(CFLAGS) $(INCLUDES) -fPIC rdma_client_lib.cpp rdma_common.o -o rdma_client_lib.o


rdma_client.o: rdma_common.o rdma_client_lib.o rdma_client.cpp rdma_client.h
	$(CC) -c $(CFLAGS) $(INCLUDES) -fPIC rdma_client.cpp -o rdma_client.o rdma_common.o

rdma_client: rdma_client.o rdma_client_lib.o
	$(CC) $(CFLAGS) $(INCLUDES) -fPIC  -o rdma_client rdma_client.o rdma_common.o rdma_client_lib.o  $(RDMA_LIBS)


rdma_server.o: rdma_server.cpp rdma_common.o
	$(CC) -c $(CFLAGS) $(INCLUDES) -fPIC rdma_server.cpp -o rdma_server.o rdma_common.o


rdma_server: rdma_server.o
	$(CC) $(CFLAGS) $(INCLUDES) -fPIC -o rdma_server rdma_server.o rdma_common.o $(RDMA_LIBS)

rdma_memory_server.o: rdma_memory_server.cpp rdma_common.o state_machines.o virtual_rdma.o search.o tables.o hash.o util.o memcached.o config.h
	$(CC) -c $(CFLAGS) $(INCLUDES) -fPIC rdma_memory_server.cpp -o rdma_memory_server.o rdma_common.o state_machines.o virtual_rdma.o search.o tables.o hash.o util.o memcached.o $(RDMA_LIBS) $(LIBRARIES)
 
rdma_memory_server: rdma_memory_server.cpp rdma_memory_server.o rdma_common.o state_machines.o virtual_rdma.o search.o tables.o hash.o util.o memcached.o config.h
	$(CC) $(CFLAGS) $(INCLUDES) -fPIC -o rdma_memory_server rdma_memory_server.o rdma_common.o state_machines.o virtual_rdma.o search.o tables.o hash.o util.o memcached.o config.o $(RDMA_LIBS) $(LIBRARIES)

memcached.o : memcached.cpp memcached.h config.h
	$(CC) -c $(CFLAGS) $(INCLUDES) -fPIC memcached.cpp -o memcached.o $(RDMA_LIBS)

cuckoo_client: cuckoo_client.cpp rdma_common.cpp cuckoo.o util.o state_machines.o rdma_engine.o rdma_common.o rdma_client_lib.o memcached.o config.h rdma_common.cpp rdma_helper.o config.o
	$(CC) $(CFLAGS) cuckoo_client.cpp -o cuckoo_client cuckoo.o rdma_engine.o tables.o hash.o search.o virtual_rdma.o util.o state_machines.o rdma_common.o rdma_client_lib.o memcached.o rdma_helper.o config.o $(LIBRARIES) $(RDMA_LIBS)

	
#rdma_common.o state_machines.o rdma_common.o state_machines.o tables.o hash.o search.o virtual_rdma.o util.o $(RDMA_LIBS)


$(test_dir)/test_hash : $(test_dir)/test_hash.cpp hash.o
#g++ $(CFLAGS) $(LIBRARIES) -lhash $(test_dir)/test_hash.cpp -o test_hash
	g++ $(CFLAGS)  $(test_dir)/test_hash.cpp -o test_hash hash.o $(LIBRARIES)
	mv test_hash $(test_dir)/test_hash

$(test_dir)/test_tables : $(test_dir)/test_tables.cpp tables.o
	g++ $(CFLAGS)  $(test_dir)/test_tables.cpp -o test_tables tables.o
	mv test_tables $(test_dir)/test_tables

$(test_dir)/test_search : $(test_dir)/test_search.cpp search.cpp search.o
	g++ $(CFLAGS)  $(test_dir)/test_search.cpp -o test_search search.o tables.o hash.o $(LIBRARIES)
	mv test_search $(test_dir)/test_search

$(test_dir)/test_state_machines : $(test_dir)/test_state_machines.cpp state_machines.o cuckoo.o rdma_helper.o config.o
	g++ $(CFLAGS)  $(test_dir)/test_state_machines.cpp -o test_state_machines state_machines.o tables.o hash.o search.o virtual_rdma.o util.o rdma_helper.o config.o $(RDMA_LIBS) $(LIBRARIES)
	mv test_state_machines $(test_dir)/test_state_machines

$(test_dir)/test_virtual_rdma : $(test_dir)/test_virtual_rdma.cpp virtual_rdma.o util.o search.o
	g++ $(CFLAGS)  $(test_dir)/test_virtual_rdma.cpp -o test_virtual_rdma tables.o hash.o search.o virtual_rdma.o util.o $(LIBRARIES)
	mv test_virtual_rdma $(test_dir)/test_virtual_rdma

$(test_dir)/test_cuckoo: $(test_dir)/test_cuckoo.cpp rdma_common.cpp cuckoo.o util.o state_machines.o rdma_engine.o rdma_common.o rdma_client_lib.o memcached.o config.h rdma_common.cpp rdma_helper.o
	g++ $(CFLAGS)  $(test_dir)/test_cuckoo.cpp -o test_cuckoo cuckoo.o rdma_engine.o tables.o hash.o search.o virtual_rdma.o util.o state_machines.o rdma_common.o rdma_client_lib.o memcached.o rdma_helper.o config.o $(LIBRARIES) $(RDMA_LIBS)
	mv test_cuckoo $(test_dir)/test_cuckoo

$(test_dir)/test_memcached: $(test_dir)/test_memcached.cpp memcached.cpp memcached.o
	$(CC) $(CFLAGS)  $(test_dir)/test_memcached.cpp -o test_memcached memcached.o $(LIBRARIES) 
	mv test_memcached $(test_dir)/test_memcached


clean:
	rm -rf test/test_hash
	rm -rf *.o *.so
	rm -rf build
	rm -rf rcuckoo_wrapper.cpp


.PHONY: all cleanclean