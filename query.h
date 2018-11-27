#ifndef QUERY_H
#define QUERY_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <stdint.h>

#include "testfiles.h"
#include "join.h"

typedef struct pred{
	int* cols;// ean exw 0.1=1.22 tote apothikevetai [0,1,-1,1,2,2] to -1 diaxwrizei ta columns
	int op; // i sxesh metaksi twn cols h col-val
	uint64_t val; // ean prokeitai gia filtro
	int isFilter;
	int isSelfjoin;
	struct pred* next;
}pred;


typedef struct query{
	int* rels; //einai poli pio aplo apo to na metraw kathe fora posa einai ta relations, kai afou tha exw 4 max to evlaa etsi
	int num_rels;
	int num_projs;
	pred* preds;
	int* projs; // ean exw 0.1 2.3 4.2 tote tha apothikevetai [0,1,2,3,4,2]
	
}query;

typedef struct batch{
	query** q_arr;
	int num_queries;
}batch;

int count_Qnum(FILE* fp,long int* offset);
int count_rels(char* rels);
int count_batches(FILE* fp);

batch* store_batch(FILE* fp,long int* offset,long int* prev_offset,char* buff, int buff_size, batch* b);
query* store_query(char* qstr, query* q);
void store_pred(char* pred_str, pred* p);
void store_proj(char* proj_str, query* q);

void reorder_preds(query* q);

void print_query(query q);
void print_batch(batch* b);

void execute_workload(char* filename, int num_loadedrels, infoNode* infoMap);
void execute_batch(batch* b, int num_loadedrels, infoNode* infoMap);
interm_node* execute_query(interm_node* interm, joinHistory** joinHist, query* q, infoNode* InfoMap, int numOfrels);
interm_node* execute_pred(interm_node* interm, joinHistory** joinHist,pred* p,int* rels, int num_loadedrels, infoNode* infoMap);

void free_batch(batch* b);
void free_query(query* q);



#endif