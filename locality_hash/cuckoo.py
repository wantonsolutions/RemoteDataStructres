import random
from hash import *
import numpy as np

class entry:
    def __init__(self, key):
        self.key = key
    def __str__(self):
        return("Key: " + str(self.key))


def cuckoo_insert(table, table_size, location_func, location_index, value, suffix, table_indexes, collisions):
    index = location_func(value, table_size, suffix)
    index = index[location_index]

    success = False
    loop=False
    if index in table_indexes:
        loop=True
    elif table[index] == None:
        table[index] = value
        success = True
    else:
        collisions+=1
        table_indexes.append(index)
        temp = table[index]
        table[index] = value
        value = temp

    return success, value, loop
            
    

def cuckoo(table_size, insertions, location_func, suffix):
    table_1 =[None] * table_size
    table_2 =[None] * table_size
    collisions_per_insert=[]

    loop = False

    rand_start=random.randint(0,table_size)

    for i in range(rand_start, insertions+rand_start):
        if loop:
            break

        success = False
        value = i
        collisions=0
        table_1_indexs=[]
        table_2_indexs=[]
        while not success:
            #base case
            success, value, loop = cuckoo_insert(table_1, table_size, location_func, 0, value, suffix, table_1_indexs, collisions)
            if success or loop:
                break

            success, value, loop = cuckoo_insert(table_2, table_size, location_func, 1, value, suffix, table_2_indexs, collisions)
            if success or loop:
                break

        collisions_per_insert.append(collisions)
        # for i in range(0, table_size):
        #     print("Table 1: " + str(table_1[i]) + "\tTable 2: " + str(table_2[i]))
        # print("-"*30)
    return collisions_per_insert



def bucket_cuckoo_insert_key(table, table_size, location_func, location_index, value, bucket_size, suffix, table_values, collisions):
    index = location_func(value.key, table_size, suffix)
    index = index[location_index]

    #print("value: " + str(value) + "\tindex: " + str(index))

    success = False
    loop=False
    #loop case
    for v in table_values:
        if v.key == value.key:
            success=False
            value=value
            loop=True
            return success, value, loop, collisions

    #search for an empty slot in this bucket
    for i in range(0, bucket_size):
        if table[index][i] == None:
            #print("found candidate index: " + '{0: <5}'.format(str(index)) + "\tvalue: " + str(value))
            table[index][i] = value
            success = True
            value=value
            loop=False
            return success, value, loop, collisions
    
    #here we have a full bucket we need to evict a candidate
    collisions+=1
    #randomly select an eviction candidate
    table_values.append(value)
    evict_index = random.randint(0, bucket_size-1)

    evict_value = table[index][evict_index]
    table[index][evict_index] = value
    value = evict_value
    return success, value, loop, collisions

def bucket_cuckoo_insert(table_1, table_2, table_size, location_func, value, bucket_size, suffix):
    collisions=0
    success=False
    table_1_values=[]
    table_2_values=[]
    v = value
    while not success:
        success, v, loop, collisions = bucket_cuckoo_insert_key(table_1, table_size, location_func, 0, v, bucket_size, suffix, table_1_values, collisions)
        if success or loop:
            break
        success, v, loop, collisions = bucket_cuckoo_insert_key(table_2, table_size, location_func, 1, v, bucket_size, suffix, table_2_values, collisions)
        if success or loop:
            break
    if loop:
        success = False

    return success, collisions


def print_bucket_table(table, table_size, bucket_size):
    for i in range(0, table_size):
        for j in range(0, bucket_size):
            print("[" + '{0: <5}'.format(str(table[i][j])) + "] ", end='')
            #print("Table: " + str(i) + "\tBucket: " + str(j) + "\tValue: " + str(table[i][j]))
        print("")

def print_styled_table(table_1,table_2, table_size, bucket_size):
    print_bucket_table(table_1, table_size, bucket_size)
    print("-"*30)
    print_bucket_table(table_2, table_size, bucket_size)
    print("-"*30)
    print("-"*30)

def generate_cuckoo_table(table_size, bucket_size):
    table = [] * table_size
    for i in range(0, table_size):
        table.append([None] * bucket_size)
    return table

def generate_cuckoo_tables(table_size, bucket_size):
    table_1 = generate_cuckoo_table(table_size, bucket_size)
    table_2 = generate_cuckoo_table(table_size, bucket_size)
    return table_1, table_2

def bucket_cuckoo_insert_only(table_size, bucket_size, insertions, location_func, suffix):
    table_1, table_2 = generate_cuckoo_tables(table_size, bucket_size)
    collisions_per_insert=[]
    values=np.arange(insertions)
    for v in values:
        v=entry(v)
        success, collisions = bucket_cuckoo_insert(table_1, table_2, table_size, location_func, v, bucket_size, suffix)
        if not success:
            break
        collisions_per_insert.append(collisions)
    #print_styled_table(table_1, table_2, table_size, bucket_size)
    return collisions_per_insert