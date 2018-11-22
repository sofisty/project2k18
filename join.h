#ifndef JOIN_H
#define JOIN_H

#include "testfiles.h"

typedef struct joinHistory{
	char* rels;
	struct joinHistory* next;
}joinHistory;

int* real_RowIds(interm_node* interm, int* rowIds, int numOfrows, int updateRel, int* newRowIds);

relation* relFromMap(infoNode* infoMap, int rel, int col);
relation* relFromInterm(interm_node* interm, int rel, int col, int indexOfrel, infoNode* infoMap);

joinHistory* add_nodeHistory(int indexOfrel, joinHistory* joinHist, int numOfrels);

#endif