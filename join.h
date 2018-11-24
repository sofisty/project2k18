#ifndef JOIN_H
#define JOIN_H

#include "testfiles.h"
#include "results.h"

typedef struct joinHistory{
	char* rels;
	struct joinHistory* next;
}joinHistory;

int* real_RowIds(interm_node* interm, int* rowIds, int numOfrows, int updateRel, int* newRowIds);

relation* relFromMap(infoNode* infoMap, int rel, int col);
relation* relFromInterm(interm_node* interm, int rel, int col, int indexOfrel, infoNode* infoMap);

joinHistory* add_nodeHistory(int indexOfrel, joinHistory* joinHist, int numOfrels);
joinHistory* update_nodeHistory(int indexOfrel, joinHistory* joinHist);

interm_node* join2(interm_node* interm, infoNode* infoMap, joinHistory** joinHist,int  rel1,int  indexOfrel1,int rel2,int indexOfrel2, int col1, int col2, int numOfrels);

#endif