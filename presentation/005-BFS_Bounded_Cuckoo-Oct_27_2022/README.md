# Introduction

I decided to check out the improvements that I could gain from using BFS to determine the cuckoo path, rather than just using dfs random search.
[http://www.cs.cmu.edu/~dga/papers/cuckoo-eurosys14.pdf](Algorithmic Improvement
for fast concurrent cuckoo Hashing), Figure 4 shows the BFS version of cuckoo
path insertion. 

After completing the BFS algorithm I ran the same fill test that I ran before running 32 experiments each, and then taking 


# Experiments

Memory Size 1024, attempt to insert up to 95% while varying the suffix and
bucket size. In this experiment I set the depth of bfs to 5. If a solution is
found at a lower depth, only solutions of that depth are returned. I chose one of the solutions at random here.

![insert_heatmap](bucket_vs_bound_bfs.png)

Note that the values here are a bit better than when we run at random.



