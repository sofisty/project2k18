#ifndef HASH2_H
#define HASH2_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

#include "hash1.h"

int hash2_func(int value,int prime);
int isPrime(int n);
int next_prime(int value);

//int bucket_chain(relation R, int start, int end, int hash_size, int** bucket, int** chain);

int buck_compare( hist_node* R_head, hist_node* S_head,psum_node* R_phead,psum_node* S_phead, int* buck_start,int* buck_end,int hash_val);
int bucket_chain(relation* R_new, int start, int end, int hash_size, int** bucket, int** chain);
int final_hash(hist_node* R_head, hist_node* S_head,psum_node* R_phead,psum_node* S_phead);

int search_offset(psum_node* phead,int hash_val);
int search_match(hist_node* bigger,hist_node* smaller,hist_node* moving);

#endif