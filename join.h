#ifndef JOIN_H
#define JOIN_H

#include "testfiles.h"
#include "results.h"


typedef struct joinHistory{
	int numOfrels;
	int** rels;
	struct joinHistory* next;
}joinHistory;

uint64_t* real_RowIds(interm_node* interm, uint64_t* rowIds, int numOfrows, int updateRel, uint64_t* newRowIds);

relation* relFromMap(infoNode* infoMap, int rel, int col);
relation* relFromInterm(interm_node* interm, int rel, int col, int indexOfrel, infoNode* infoMap);

joinHistory* add_nodeHistory(int indexOfrel, int joinedRel, joinHistory* joinHist, int numOfrels);
joinHistory* update_nodeHistory(int indexOfrel, int joinedRel, joinHistory* joinHist);
int print_joinHist(joinHistory* joinHist);
int find_join(joinHistory* joinHist, int indexOfrel1, int indexOfrel2);

joinHistory* delete_nodeHistory(int indexOfrel, joinHistory** joinHist);
joinHistory* merge_nodeHistory(int indexOfrel1, int indexOfrel2, joinHistory* new, joinHistory** joinHist);

uint64_t** exec_join(interm_node* interm, infoNode* infoMap, int  rel1,int  indexOfrel1,int rel2,int indexOfrel2, int col1, int col2, int numOfrels, uint64_t** rowIds1, uint64_t** rowIds2, int* numOfrows);
interm_node* special_sjoin(interm_node* interm, infoNode* infoMap, int  rel1,int  indexOfrel1,int rel2,int indexOfrel2, int col1, int col2, int numOfrels);
interm_node* join2(interm_node* interm, infoNode* infoMap, joinHistory** joinHist,int rel1,int indexOfrel1,int rel2,int indexOfrel2, int col1, int col2, int numOfrels);

#endif