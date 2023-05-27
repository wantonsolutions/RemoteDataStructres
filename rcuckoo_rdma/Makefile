# # location of the Python header files
# # PYTHON_VERSION = 3.8
# # PYTHON_VERSIONNS = 38
# PYTHON_INCLUDE = /usr/include/python$(PYTHON_VERSION)
# # location of the Boost Python include files and library
# BOOST_INC = /usr/local/home/ssgrant/boost_1_82_0/include
# # BOOST_LIB = /usr/local/home/ssgrant/boost_1_82_0/lib
# XXHASH_INC = /usr/local/home/ssgrant/RemoteDataStructres/rcuckoo_rdma/xxHash

# CPP_FLAGS = -DFAST
# # CPP_FLAGS += -DDEBUG
# # compile mesh classes

# TARGET = tables
# # TARGET = hash
# all: $(TARGET)


# $(TARGET).so: $(TARGET).o
# 	g++ -shared -Wl,--export-dynamic $(TARGET).o -L$(XXHASH_INC) -lxxhash -o $(TARGET).so -lssl -lcrypto 
# $(TARGET).o: $(TARGET).cpp
# # g++ -I$(PYTHON_INCLUDE) -I$(BOOST_INC)  -I$(XXHASH_INC) $(CPP_FLAGS) -fPIC -c $(TARGET).cpp
# 	g++ -o $(TARGET).o -I$(XXHASH_INC) $(CPP_FLAGS) -fPIC -c $(TARGET).cpp 

# clean:
# 	rm -f *.o *.so
# 	rm -rf build
# 	rm -f hash_wrapper.cpp
# 	rm -f tables_wrapper.cpp

NAME = tables

CC = g++ # define the C/C++ compiler to use
CFLAGS = -O3 -Wall -Wall -Wextra -pedantic -fPIC
# define any directories containing header files other than /usr/include
XXHASH_INC = /usr/local/home/ssgrant/RemoteDataStructres/rcuckoo_rdma/xxHash
INCLUDES = -I$(XXHASH_INC)
# define the C++ source files
SRCS = $(NAME).cpp 
# define the C/C++ object files 
# This uses Suffix Replacement within a macro:
#   $(name:string1=string2)
#         For each word in 'name' replace 'string1' with 'string2'
OBJS = $(SRCS:.c=.o)
# define the shared library name
TARGET = $(NAME).so

.PHONY: clean
    
all:    $(TARGET)
	@echo  Successfully compiled a .so library.

# compile object file to .so shared library
$(TARGET): $(OBJS) 
	$(CC) $(CFLAGS) $(INCLUDES) -shared -o $(TARGET) $(OBJS) 

# compile source files to object files

# this is a suffix replacement rule for building .o's from .c's
# it uses automatic variables $<: the name of the prerequisite of
# the rule(a .c file) and $@: the name of the target of the rule (a .o file) 
# (see the gnu make manual section about automatic variables)
.c.o:
	$(CC) $(CFLAGS) $(INCLUDES) -cpp $<  -o $@
clean:
	rm -r *.o *.so
	rm -r build
	rm -r hash_wrapper.cpp
	rm -r tables_wrapper.cpp