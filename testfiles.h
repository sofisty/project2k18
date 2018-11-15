#ifndef FILES_H
#define FILES_H

# include <stdio.h>
# include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdint.h>

typedef struct RelFiles {
	char file[250];
	struct RelFiles* next; 
} RelFiles;

typedef struct infoNode {
	uint64_t columns;
	uint64_t tuples;
	uint64_t* addr;
} infoNode;


RelFiles* add_Relation(RelFiles** relHead, RelFiles* relList, char* file);
void print_RelFiles(RelFiles* relList);
infoNode* create_InfoMap(RelFiles* relList, infoNode* infoMap, int numOffiles );
void print_InfoMap(infoNode* infoMap, int numOffiles);




#endif