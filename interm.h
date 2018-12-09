#ifndef INTERM_H
#define INTERM_H

#include "infomap.h"


typedef struct interm_node{
	uint64_t** rowIds;
	int* numOfrows;
	int numOfrels;
}interm_node;

interm_node* filter(interm_node* interm,int oper, infoNode* infoMap, int rel, int indexOfrel, int col, uint64_t value, int numOfrels);
interm_node* self_join(interm_node* interm, infoNode* infoMap, int rel, int indexOfrel, int col1, int col2, int numOfrels);

interm_node* store_interm_data(interm_node* interm ,uint64_t* rowIds, int indexOfrel, int numOfrows, int numOfrels);
interm_node* update_interm(interm_node* interm, uint64_t* rowIds, int indexOfrel, int numOfrows,int numOfrels);
void free_interm(interm_node* interm);

uint64_t* filterFromInterm(interm_node* interm, int oper, uint64_t value,  int rel, int indexOfrel, int col,infoNode* infoMap, uint64_t* filterRowIds, int* numOfrows);
uint64_t* selfjoinFromInterm(interm_node* interm, int rel, int indexOfrel, int col1, int col2, infoNode* infoMap,uint64_t* sjoinRowIds, int* numOfrows);

void statusOfinterm(interm_node* interm);

#endif