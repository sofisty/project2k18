#ifndef QUERY_H
#define QUERY_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <stdint.h>

#include <cstdlib>


#include "join.h"
#include "optim.h"




typedef struct query{
	int* rels; 
	int num_rels;
	int num_projs;
	pred* preds;
	int* projs; // ean exw 0.1 2.3 4.2 tote tha apothikevetai [0,1,-1,2,3,-1,4,2,-1]
	
}query;


typedef struct query_list {
	char query[500];
	struct query_list* next; 
} query_list;

typedef struct batch{
	query** q_arr;
	int num_queries;
}batch;


query_list* add_quNode(query_list** quHead, query_list* quList, char* buff);
void free_quList(query_list* quList);
void print_quList(query_list* quList);

int count_Qnum(FILE* fp,long int* offset);
int count_rels(char* rels);
int count_batches(FILE* fp);

batch* store_batch(query_list* quList, int num_queries, batch* b);
query* store_query(char* qstr, query* q);
void store_pred(char* pred_str, pred* p);
void store_proj(char* proj_str, query* q);

void reorder_preds(query* q);
pred* reorder_priority(pred* joinHead);

void print_query(query q);
void print_batch(batch* b);

void execute_workload( int num_loadedrels, infoNode* infoMap);
void execute_batch(batch* b, int num_loadedrels, infoNode* infoMap, int* numquery);
interm_node* execute_query(interm_node* interm, joinHistory** joinHist, query* q, infoNode* InfoMap, int numOfrels);
interm_node* execute_pred(interm_node* interm, joinHistory** joinHist,pred* p,int* rels, int num_loadedrels, infoNode* infoMap, stats** qu_stats);

void proj_sums(interm_node* interm, query* q, infoNode* infoMap);
void proj_sumsAfterCross(long long int* toMul, interm_node* interm, query* q, infoNode* infoMap);

void free_batch(batch* b);
void free_query(query* q);




#endif