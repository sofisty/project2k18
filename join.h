#ifndef JOIN_H
#define JOIN_H

#include "interm.h"
#include "results.h"


typedef struct joinHistory{
	int numOfrels;
	int** rels;
	struct joinHistory* next;
}joinHistory;

typedef struct cross_list{
	int* numOfrows;
	uint64_t** rowIds;
	long long int* toMul;
	int numOfrels;
}cross_list;

uint64_t* real_RowIds(interm_node* interm, uint64_t* rowIds, int numOfrows, int updateRel, uint64_t* newRowIds);

relation* relFromMap(infoNode* infoMap, int rel, int col);
relation* relFromInterm(interm_node* interm, int rel, int col, int indexOfrel, infoNode* infoMap);

joinHistory* add_nodeHistory(int indexOfrel, int joinedRel, joinHistory* joinHist, int numOfrels);
joinHistory* update_nodeHistory(int indexOfrel, int joinedRel, joinHistory* joinHist);
int print_joinHist(joinHistory* joinHist);
int find_join(joinHistory* joinHist, int indexOfrel1, int indexOfrel2);
void free_joinistory(joinHistory* joinHist);

joinHistory* delete_nodeHistory( joinHistory** joinHist, joinHistory* currHist);
joinHistory* merge_nodeHistory( joinHistory* newj, joinHistory** joinHist);

uint64_t** exec_join(interm_node* interm, infoNode* infoMap, int  rel1,int  indexOfrel1,int rel2,int indexOfrel2, int col1, int col2, uint64_t** rowIds1, uint64_t** rowIds2, int* numOfrows, int* free_flag);
interm_node* special_sjoin(interm_node* interm, infoNode* infoMap, int  rel1,int  indexOfrel1,int rel2,int indexOfrel2, int col1, int col2, int numOfrels, uint64_t** sj_updateIds, int* new_numOfrows);
interm_node* join2(interm_node* interm, infoNode* infoMap, joinHistory** joinHist,int rel1,int indexOfrel1,int rel2,int indexOfrel2, int col1, int col2, int numOfrels);

long long int* cross_nodes(interm_node* interm,int* q_rels, infoNode* infoMap, joinHistory** joinHist, int numOfrels);
long long int* init_crossArr(long long int* arr, int numOfrels);
void statusOfCross(interm_node* interm, long long int* toMul, int numOfrels);

#endif