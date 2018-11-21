#ifndef TESTFILES_H
#define TESTFILES_H

# include <stdio.h>
# include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdint.h>

#include "hash1.h"

typedef struct RelFiles {
	char file[250];
	struct RelFiles* next; 
} RelFiles;

typedef struct infoNode {
	uint64_t columns;
	uint64_t tuples;
	uint64_t* addr;
} infoNode;

typedef struct interm_node{
	int** rowIds;
	int* numOfrows;
	int numOfrels;
}interm_node;

RelFiles* add_Relation(RelFiles** relHead, RelFiles* relList, char* file);
void print_RelFiles(RelFiles* relList);
infoNode* create_InfoMap(RelFiles* relList, infoNode* infoMap, int numOffiles );
void print_InfoMap(infoNode* infoMap, int numOffiles);

interm_node* filter(interm_node* interm,int oper, infoNode* infoMap, int rel, int indexOfrel, int col, uint64_t value, int numOfrels);
interm_node* self_join(interm_node* interm, infoNode* infoMap, int rel, int indexOfrel, int col1, int col2, int numOfrels);


interm_node* store_interm_data(interm_node* interm ,int* rowIds, int indexOfrel, int numOfrows, int numOfrels);


interm_node* update_intermFilSjoin(interm_node* interm, int* rowIds, int indexOfrel, int numOfrows,int numOfrels);

uint64_t return_value(infoNode* infoMap, int rel ,int col, int tuple);

int* filterFromInterm(interm_node* interm, int oper, uint64_t value,  int rel, int indexOfrel, int col,infoNode* infoMap, int* filterRowIds, int* numOfrows);
int* filterFromRel(int oper,uint64_t value,uint64_t* ptr, int numOftuples,int* filterRowIds, int* numOfrows);
int* selfjoinFromRel(uint64_t* ptr1, uint64_t* ptr2, int numOftuples, int* sjoinRowIds, int* numOfrows);
int* selfjoinFromInterm(interm_node* interm, int rel, int indexOfrel, int col1, int col2, infoNode* infoMap,int* sjoinRowIds, int* numOfrows);



#endif