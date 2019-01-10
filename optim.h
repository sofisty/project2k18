#ifndef OPTIM_H
#define OPTIM_H

#include "query.h"

typedef struct treeNode {
	int* path;
	stats* path_stats;
	double cost;
	
} treeNode;

int* joinEnumeration(int numOfrels, struct joinHash*  jh, stats* qu_stats);
stats* copy_stats(stats* qu_stats, int numOfrels);
double createJoinTRee(treeNode joinTree, treeNode* newTree , int rel1, int rel2,joinHash* jh, int k, int numOfrels );

#endif