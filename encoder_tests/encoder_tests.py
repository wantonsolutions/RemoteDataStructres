
def reverse_hex_string_in_bytes(hex_string):
    assert(len(hex_string) == 8)
    return hex_string[6:8] + hex_string[4:6] + hex_string[2:4] + hex_string[0:2]

def encode_entry_to_string(a):
    key = '{0:08X}'.format(a)
    key = reverse_hex_string_in_bytes(key)
    dummy_value = "0000"
    return key + ":" + dummy_value

def encode_entries_to_string(entries):
    str_list = ""
    for i in range(0, len(entries)):
        str_list += encode_entry_to_string(entries[i])
        if i != len(entries) - 1:
            str_list += ","
    return str_list.encode('utf8')


values = [0,2,3,4,5,6,7]
print(encode_entries_to_string(values))


def encode_entry_from_binary_string(a):
    key = '{0:08X}'.format(int(str(a), 16))
    key = reverse_hex_string_in_bytes(key)
    dummy_value = "0000"
    return key + ":" + dummy_value

entry = 1
print(encode_entry_from_binary_string(entry))