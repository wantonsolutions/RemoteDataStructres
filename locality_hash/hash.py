import hashlib

def h1(key):
    return hashlib.md5(str(key).encode('utf-8')).hexdigest()

def h2(key):
    val = str(key) + "salt"
    return hashlib.sha1(val.encode('utf-8')).hexdigest()

def h3(key):
    val = str(key) + "salty"
    return hashlib.sha1(val.encode('utf-8')).hexdigest()

def primary_location(key, table_size):
    return int(h1(key),16) % table_size


def secondary_location(key, table_size):
    return int(h2(key),16) % table_size

def h3_suffix(key):
    val = int(h3(key),32)
    val = str(bin(val))[2:]
    zeros = len(val) - len(val.rstrip('0'))
    return zeros

def h3_phi_suffix(key):
    val = int(h3(key),32)
    phi_mods = [1, 3, 5, 10, 19, 35, 64, 117, 213, 387, 704, 1277, 2319, 4209, 7640, 13868, 25171, 45685, 82919, 150498, 273155, 495776, 899834, 1633198, 2964256, 5380124, 9764926, 17723342, 32167866, 58384676, 105968188]
    for i in range(len(phi_mods)):
        if val % phi_mods[i] != 0:
            return i
    return len(phi_mods)

def secondary_bounded_location(key, table_size, suffix_size):
    primary = primary_location(key, table_size)
    secondary = (int(h2(key),16)) % suffix_size
    return (primary + secondary) % table_size

def secondary_bounded_location_exp(key, table_size, suffix_size):
    primary = primary_location(key, table_size)
    #this works
    zeros = (h3_suffix(key) + 1) * suffix_size
    zeros = (2 ** zeros)
    secondary = (int(h2(key),16)) % zeros
    ##
    return (primary + secondary) % table_size

def secondary_bounded_location_phi(key, table_size, suffix_size):
    primary = primary_location(key, table_size)
    #zeros = (h3_phi_suffix(key) * (1.815))
    exp = (h3_phi_suffix(key) + 2)
    mod_size = int((1.815 ** exp))
    secondary = (int(h2(key),16)) % mod_size
    return (primary + secondary) % table_size


def get_locations(key, table_size, suffix):
    return (primary_location(key, table_size), secondary_location(key, table_size))

def get_locations_bounded(key, table_size, suffix_size):
    return (primary_location(key, table_size), secondary_bounded_location(key, table_size, suffix_size))

def get_locations_bounded_exp(key, table_size, suffix_size):
    return (primary_location(key, table_size), secondary_bounded_location_exp(key, table_size, suffix_size))

def get_locations_bounded_phi(key, table_size, suffix_size):
    return (primary_location(key, table_size), secondary_bounded_location_phi(key, table_size, suffix_size))




##### Test Functions
def to_base(n, base):
    if n == 0:
        return '0'
    nums = []
    while n:
        n, r = divmod(n, base)
        nums.append(str(r))
    return ''.join(reversed(nums))

def h3_suffix_base(key, base):
    val = int(h3(key),32)
    val = to_base(val,base)
    zeros = len(val) - len(val.rstrip('0'))
    return zeros

def secondary_bounded_location_exp_test(key, table_size, suffix_size, base, exponential):
    primary = primary_location(key, table_size)
    zeros = (h3_suffix_base(key,base) + 1) * suffix_size
    #how do increase bin size?
    zeros = int((exponential ** zeros))
    # if zeros > 32:
    #     zeros = 32
    secondary = (int(h2(key),16)) % zeros
    #print(zeros)
    #secondary = (int(h2(key),16)) % int((2**zeros) / 2) #does not really work
    # secondary = int(secondary/2) #todo play with this line to see how it affects the performance
    return (primary + secondary) % table_size

def get_locations_bounded_test(key, table_size, suffix, base, exponential):
    return (primary_location(key, table_size), secondary_bounded_location_exp_test(key, table_size, suffix, base, exponential))
