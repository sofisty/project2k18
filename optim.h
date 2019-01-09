#ifndef OPTIM_H
#define OPTIM_H

#include "query.h"

//den eimai katholou sigourh gia auto
typedef struct joinTree_node{
	int degree;
	int* path;
	struct joinTree_node* parent;
	struct joinTree_node* child;
}joinTree_node;

typedef struct joinIndex{
	int numOfrels;
	int** posOfJoins; //o nxn pinakas pou krataei se poio bucket vrisketai to join metaksi 2 relations
	int** relCombs;	//oi n ana k pithanes diades metaksi twn relations
	pred** joinBuckets; //array apo buckets 
	int* done; //simatodotis done gia kathe bucket
}joinIndex;

int factorial(int n);
int* joinEnumeration(joinIndex* index, joinTree_node* root);

