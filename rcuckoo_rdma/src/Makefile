TARGET := rdma_common.o rdma_client.o
CFLAGS = -std=c++2a -O3 -Wall -Wall -Wextra -pedantic -fPIC

# CC=gcc
CC=g++

all: $(TARGET)

rdma_common.o:
	$(CC) -c $(CFLAGS) $(INCLUDES) -fPIC rdma_common.cpp -o rdma_common.o

rdma_client.o: rdma_common.o
	$(CC) -c $(CFLAGS) $(INCLUDES) -fPIC rdma_client.cpp -o rdma_client.o rdma_common.o

clean:
	rm -rf *.o *.so
