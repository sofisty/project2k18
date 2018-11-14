#ifndef HASH1_H
#define HASH1_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <stdint.h>


typedef struct tuple {
	uint64_t key;
	uint64_t payload;
} tuple;

typedef struct relation {
	tuple *tuples;
	uint64_t num_tuples;
} relation;


typedef struct hist_node {
    int hash_val;
    int count;
} hist_node;

typedef struct psum_node {
    int hash_val;
    int offset;
    int curr_off;
} psum_node;

typedef struct result {
    uint64_t *matches;
    struct result* next;
    int count;
} result;

int hash_func(uint64_t value, int n);

hist_node* update_hist(hist_node* hist, relation* R, int n);
void print_hist(hist_node*  hist);


psum_node* update_psumlist(psum_node* psum, hist_node* hist );
void print_psum(psum_node * psum);

relation* reorder_R(psum_node* phead, relation* R, relation* R_new, int n  );
void print_R(relation* R);




#endif