import numpy as np
# t=0.99
a=1.000001
sum=0
s = np.random.zipf(a, 10000)
copy = []
for i in range(len(s)):
    if s[i] < 100000:
        copy.append(s[i])

print(copy)

#     print(sum)
#     if i != 1:
#         sum=sum+1
#         print(sum)
# print("printing sum")
# print(sum)
# result = (s/float(max(s)))*1000

# print (min(s), max(s))
# print (min(result), max(result))
