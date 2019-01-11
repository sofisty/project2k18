#ifndef HASH2_H
#define HASH2_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <stdint.h>

#include "hash1.h"
#include "results.h"

int hash2_func(uint64_t value,int prime);
int isPrime(int n);
int next_prime(int value);

int buck_compare( hist_node R_hist, hist_node S_hist);
int bucket_chain(relation* R_new, int start, int end, int hash_size, int** bucket, int** chain);
void free_bucket_chain(int** bucket, int** chain, int hash_size, int bucket_size);

result* join(result** head,result* curr_res, int index,hist_node curr_R, hist_node curr_S, psum_node curr_Rp, psum_node curr_Sp, relation* R_new, relation* S_new);



#endif