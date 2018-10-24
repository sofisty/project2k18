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

int buck_compare( hist_node* R_head, hist_node* S_head,psum_node* R_phead,psum_node* S_phead);
int bucket_chain(relation* R_new, int start, int end, int hash_size, int** bucket, int** chain);
void free_bucket_chain(int** bucket, int** chain, int hash_size, int bucket_size);
int final_hash(hist_node* R_head, hist_node* S_head,psum_node* R_phead,psum_node* S_phead, relation* R_new, relation* S_new);

//int search_offset(psum_node* phead,int hash_val);
int search_match(hist_node** current_R,  hist_node** current_S, psum_node** current_Rp, psum_node** current_Sp );

result*	join(result* result_list, int index,hist_node* curr_R, hist_node* curr_S, psum_node* curr_Rp, psum_node* curr_Sp, relation* R_new, relation* S_new);



#endif