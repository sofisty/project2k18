#ifndef OPTIM_H
#define OPTIM_H
#include "interm.h"


typedef struct pred{
	int rels[2];//ean exw 0.1=1.22 tote apothikevetai [0,1] 
	int cols[2];// ean exw 0.1=1.22 tote apothikevetai [1,2]
	int op; // i sxesh metaksi twn cols h col-val 1--> > , 2--> <, 3--> =
	uint64_t val; // ean prokeitai gia filtro
	int isFilter;
	int isSelfjoin;
	int priority;
	struct pred* next;
}pred;


typedef struct treeNode {
	int path;
	stats* path_stats;
	double cost;
	pred* predl;
	
} treeNode;





//int joinEnumeration(int numOfrels, struct pred** predl, stats* qu_stats);
void free_joinTree(treeNode* joinTree, int size, int numOfrels);
void free_predl(pred* head);
void add_predl(pred** head, pred* new_pred);
 void add_pred(pred** head, pred* new_pred);
 void replace_pred(pred** head, pred* new_pred);
pred* copy_predl( pred* predl);
void print_predList(pred* head);
stats* copy_stats(stats* qu_stats, int numOfrels);
 pred* joinEnumeration(int numOfrels, pred* predl, stats* qu_stats, treeNode** joinT);
 void print_joinTree(treeNode* joinTree, int size);


#endif