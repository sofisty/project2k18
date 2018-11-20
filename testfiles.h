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
	int* rels;
	int numOfrels;
	int** rowIds;
	int numOfrows;
	struct interm_node* next;
}interm_node;

RelFiles* add_Relation(RelFiles** relHead, RelFiles* relList, char* file);
void print_RelFiles(RelFiles* relList);
infoNode* create_InfoMap(RelFiles* relList, infoNode* infoMap, int numOffiles );
void print_InfoMap(infoNode* infoMap, int numOffiles);

interm_node* filter(interm_node* interm ,char oper, infoNode* infoMap, int rel, int col, uint64_t value);
interm_node* store_interm_data(interm_node* interm, int posOfrel, int numOfrows, int rel, int* rowIds,int numOfrels);
interm_node* update_intermFilter(interm_node** interm_header, int* rowIds, int rel, int numOfrows, interm_node* new);
interm_node* search_interm(int rel, interm_node* interm, int* posOfrel);
void print_interm(interm_node* interm);
void free_intermNode(interm_node* interm);

uint64_t return_value(infoNode* infoMap, int rel ,int col, int tuple);

int* filterFromInterm(interm_node* curr_interm, char oper, uint64_t value,  int rel, int col,infoNode* infoMap, int* filterRowIds, int* numOfrows);
int* filterFromRel(char oper,uint64_t value,uint64_t* ptr, int numOftuples,int* filterRowIds, int* numOfrows);



#endif