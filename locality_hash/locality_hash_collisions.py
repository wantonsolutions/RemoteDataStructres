def single(table, primary, secondary):
    table[primary]+=1

def blind(table, primary, secondary):
    if table[primary] == 0:
        table[secondary]+=1
    else:
        table[secondary]+=1

def less_crowded(table, primary, secondary):
    if table[primary] < table[secondary]:
        table[primary]+=1
    else:
        table[secondary]+=1