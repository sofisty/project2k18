#ifndef TESTING_H
#define TESTING_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>

#include "hash1.h"
#include "files.h"


void store_test_res(result* result_list, FILE* fp, char* filename, int* resfortest);
int testing(void);

#endif