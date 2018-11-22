#ifndef JOIN_H
#define JOIN_H

#include "testfiles.h"

typedef struct joinHistory{
	char* rels;
	struct joinHistory* next;
}joinHistory;

int* real_RowIds(interm_node* interm, int* rowIds, int numOfrows, int updateRel, int* newRowIds);
joinHistory* add_nodeHistory(int indexOfrel, joinHistory* joinHist, int numOfrels);

#endif