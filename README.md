# OSThreadsParallelSort
Project Description: https://github.com/remzi-arpacidusseau/ostep-projects/tree/master/concurrency-sort

- We split the array into threads=get_nprocs() number of subarrays and sort them paralelly. Each of the threads sort their respective subarrays.
- we then merge all the results into one array using 'mergeResults' function which recursively merges the results from all the threads
- We optimised the code by calling pthread_join from with the mergeResults function. This way merging can begin even before some of the threads have completed sorting. This way we don't have to wait for the slowest threads.
