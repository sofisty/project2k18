#ifndef OPTIM_H
#define OPTIM_H

#include "query.h"

typedef struct treeNode {
	int* path;
	stats* path_stats;
	double cost;
	
} treeNode;

int joinEnumeration(int numOfrels, struct pred** predl, stats* qu_stats);
stats* copy_stats(stats* qu_stats, int numOfrels);


#endif