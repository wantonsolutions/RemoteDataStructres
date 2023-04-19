import hash

def test_hash():

    tests = []
    table_size = 15

    input_index = 0
    output = (0,1)
    tests.append((input_index, output))

    input_index = 1
    output = (2,1)
    tests.append((input_index, output))

    input_index = 2
    output = (3,4)
    tests.append((input_index, output))

    input_index = 3
    output = (5,4)
    tests.append((input_index, output))


    for t in tests:
        
        input_index = t[0]
        output = t[1]
        index, overflow = hash.to_race_index_math(input_index, table_size)

        assert index == output[0], "index: %s, test_index: %s input_index %s" % (index, output[0], input_index)
        assert overflow == output[1], "overflow: %s, output: %s input_index %s" % (overflow, output[1], input_index)

test_hash()