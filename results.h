#ifndef RESULTS_H
#define RESULTS_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#include "hash1.h"
#include "hash2.h"
#include "jobScheduler.h"


result* search_results(result** head,result* curr_res, relation* S_new, int startS, int endS, int** bucket, int** chain, relation* R_new, int startR, int hash_size,  int index);
result* store_results(result** head,result* curr_res, tup resultR, tup resultS );
void print_results(result* result_list,int* resfortest);
uint64_t** resToRowIds(result* result_list, int* numOfrows);
void free_rowIds(uint64_t** rowIds);
void free_result_list(result* result_list);
result* copy_results(result** resultList_arr,result* result_list);

result* RadixHashJoin(relation *R, relation *S);

#endif
