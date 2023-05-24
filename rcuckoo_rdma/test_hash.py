# import pyximport
# pyximport.install()

import cython

# from chash import set_factor

# import chash as h
import rcuckoo_rdma as h
#from chash import set_factor_ref
from inspect import getmembers, isfunction

print(getmembers(h), isfunction)
print(dir(h))

value = 2.5
print("value: ", value)
h.set_factor(value)
h.set_factor(value+1)

value = h.get_factor()
print("value: ", value)
# h.set_factorRf(2)
# h._Z10set_factorRf(2)

# set_factor_ref(2)

