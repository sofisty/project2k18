#ifndef FILES_H
#define FILES_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

#include "hash1.h"



int count_lines(FILE *fp);
int store_file(FILE* fp,char* buff,int buff_size,tuple* rel_t,int lines);

#endif