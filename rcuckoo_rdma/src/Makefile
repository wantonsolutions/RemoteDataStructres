TARGET := rdma_common.o rdma_client rdma_server
CFLAGS = -std=c++2a -O3 -Wall -Wall -Wextra -pedantic -fPIC
LIBS = -lrdmacm -libverbs -lpthread

# CC=gcc
CC=g++

all: $(TARGET)

rdma_common.o:
	$(CC) -c $(CFLAGS) $(INCLUDES) -fPIC rdma_common.cpp -o rdma_common.o

rdma_client.o: rdma_common.o
	$(CC) -c $(CFLAGS) $(INCLUDES) -fPIC rdma_client.cpp -o rdma_client.o rdma_common.o

rdma_server.o: rdma_common.o
	$(CC) -c $(CFLAGS) $(INCLUDES) -fPIC rdma_server.cpp -o rdma_server.o rdma_common.o

rdma_server: rdma_server.o
	$(CC) $(CFLAGS) $(INCLUDES) -fPIC rdma_server.o rdma_common.o -o rdma_server $(LIBS)

rdma_client: rdma_client.o
	$(CC) $(CFLAGS) $(INCLUDES) -fPIC rdma_client.o rdma_common.o -o rdma_client $(LIBS)

clean:
	rm -rf *.o *.so
