# NAME = tables hash

# CC = g++ # define the C/C++ compiler to use
# CFLAGS = -O3 -Wall -Wall -Wextra -pedantic -fPIC
# # define any directories containing header files other than /usr/include
XXHASH_INC = /usr/local/home/ssgrant/RemoteDataStructres/rcuckoo_rdma/xxHash
INCLUDES = -I$(XXHASH_INC)
LIBRARIES = -L$(XXHASH_INC) -lxxhash


CFLAGS = -std=c++2a -O3 -Wall -Wall -Wextra -pedantic -fPIC
RDMA_LIBS = -lrdmacm -libverbs -lpthread
# # define the C++ source files
# SRCS = $(NAME).cpp 
# # define the C/C++ object files 
# # This uses Suffix Replacement within a macro:
# #   $(name:string1=string2)
# #         For each word in 'name' replace 'string1' with 'string2'
# OBJS = $(SRCS:.c=.o)
# # define the shared library name
# TARGET = $(NAME).so

# .PHONY: clean
    
# all:    $(TARGET)
# 	@echo  Successfully compiled a .so library.

# # compile object file to .so shared library
# $(TARGET): $(OBJS) 
# 	$(CC) $(CFLAGS) $(INCLUDES) -shared -o $(TARGET) $(OBJS) 

# # compile source files to object files

# # this is a suffix replacement rule for building .o's from .c's
# # it uses automatic variables $<: the name of the prerequisite of
# # the rule(a .c file) and $@: the name of the target of the rule (a .o file) 
# # (see the gnu make manual section about automatic variables)
# .c.o:
# 	$(CC) $(CFLAGS) $(INCLUDES) -cpp $<  -o $@

TARGET := test_hash test_tables test_search rdma_server rdma_client
test_dir := ./test

all: $(TARGET)

hash.o : hash.cpp hash.h
	g++ -c $(CFLAGS) $(INCLUDES) -fPIC hash.cpp -o hash.o 

tables.o: tables.cpp tables.h
	g++ -c $(CFLAGS) $(INCLUDES) -fPIC tables.cpp -o tables.o

search.o: search.cpp search.h tables.o hash.o
	g++ -c $(CFLAGS) $(INCLUDES) -fPIC search.cpp -o search.o tables.o hash.o

rdma_common.o:
	$(CC) -c $(CFLAGS) $(INCLUDES) -fPIC rdma_common.cpp -o rdma_common.o

rdma_client.o: rdma_common.o
	$(CC) -c $(CFLAGS) $(INCLUDES) -fPIC rdma_client.cpp -o rdma_client.o rdma_common.o

rdma_server.o: rdma_common.o
	$(CC) -c $(CFLAGS) $(INCLUDES) -fPIC rdma_server.cpp -o rdma_server.o rdma_common.o

rdma_server: rdma_server.o
	$(CC) $(CFLAGS) $(INCLUDES) -fPIC rdma_server.o rdma_common.o -o rdma_server $(RDMA_LIBS)

rdma_client: rdma_client.o
	$(CC) $(CFLAGS) $(INCLUDES) -fPIC rdma_client.o rdma_common.o -o rdma_client $(RDMA_LIBS)


test_hash : hash.o
#g++ $(CFLAGS) $(LIBRARIES) -lhash $(test_dir)/test_hash.cpp -o test_hash
	g++ $(CFLAGS)  $(test_dir)/test_hash.cpp -o test_hash hash.o $(LIBRARIES)
	mv test_hash $(test_dir)/test_hash

test_tables : tables.o
	g++ $(CFLAGS)  $(test_dir)/test_tables.cpp -o test_tables tables.o
	mv test_tables $(test_dir)/test_tables

test_search : search.o
	g++ $(CFLAGS)  $(test_dir)/test_search.cpp -o test_search search.o tables.o hash.o $(LIBRARIES)
	mv test_search $(test_dir)/test_search


clean:
	rm -rf test/test_hash
	rm -rf *.o *.so
	rm -rf build
	rm -rf rcuckoo_wrapper.cpp