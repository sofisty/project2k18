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

int hash_func(int value, int n);
hist_node*  update_hist(hist_node* head, int hash_val);
void print_hist(hist_node*  head);

psum_node* add_psum_node(psum_node* head,  int hash_val, int offset);
psum_node* create_psumlist(psum_node* psum_head, hist_node* hist_head);
void print_psum(psum_node * head);
int search_Psum(psum_node* head, int key, int n );

relation* reorder_R(psum_node* phead, relation* R, relation* R_new, int n  );
void print_R(relation* R);

int bucket_chain(tuple* tuples,int bucket_size, int hash_size, int** bucket, int** chain);


#endif