#ifndef HASH1_H
#define HASH1_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

typedef struct tuple {
	int32_t key;
	int32_t payload;
} tuple;

typedef struct relation {
	tuple *tuples;
	int32_t num_tuples;
} relation;


typedef struct hist_node {
    int hash_val;
    int count;
    struct hist_node * next;
} hist_node;

typedef struct psum_node {
    int hash_val;
    int offset;
    int curr_off;

    struct psum_node * next;
} psum_node;

typedef struct result {
	tuple* tuplesR;
    tuple* tuplesS;
	struct result* next;
    int count;
} result;

int hash_func(int value, int n);
hist_node*  update_hist(hist_node* head, int hash_val, int* total_buckets);
void print_hist(hist_node*  head);

psum_node* add_psum_node(psum_node* head,  int hash_val, int offset);
psum_node* create_psumlist(psum_node* psum_head, hist_node* hist_head);
void print_psum(psum_node * head);
int search_Psum(psum_node* head, int key, int n );

relation* reorder_R(psum_node* phead, relation* R, relation* R_new, int n  );
void print_R(relation* R);

int hash2_func(int value,int prime);
int isPrime(int n);
int next_prime(int value);

//int bucket_chain(relation R, int start, int end, int hash_size, int** bucket, int** chain);
int bucket_chain(relation* R_new, int start, int end, int hash_size, int** bucket, int** chain);

//result* search_results(result* result_list,tuple* bucketS, int bucketS_size, int** bucket, int** chain, tuple* bucketR, int hash_size, int bucketR_size); //TA DUO RELATION ARXH KAI TELOS KAI TWN DUO
result* search_results(result* result_list, relation* S_new, int startS, int endS, int** bucket, int** chain, relation* R_new, int hash_size,  int index);
result* store_results(result* result_list, tuple resultR, tuple resultS );
void print_results(result* result_list);


#endif