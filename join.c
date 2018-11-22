#include "join.h"

int* real_RowIds(interm_node* interm, int* rowIds, int numOfrows, int updateRel, int* newRowIds){
  int i, j; 
  newRowIds=malloc(numOfrows* sizeof(int));
  if(newRowIds==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}
  for(i=0; i<numOfrows; i++){
    j=rowIds[i];
    newRowIds[i]=interm->rowIds[updateRel][j];

  }

  return newRowIds;
}

relation* relFromMap(infoNode* infoMap, int rel, int col){
  int i, numOftuples;
  uint64_t* ptr;

  numOftuples= infoMap[rel].tuples;

  relation* r=malloc(sizeof(relation));
  if(r==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}

  r->num_tuples=(uint64_t)numOftuples;
  r->tuples=malloc(numOftuples*sizeof(tuple));
  if(r->tuples==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}

  ptr=(uint64_t*)infoMap[rel].addr[col];

  for(i=0; i<numOftuples; i++){
    r->tuples[i].payload=(uint64_t)i;
    r->tuples[i].key=*ptr;
    ptr++;
  }
  if(r==NULL) return NULL;

  return r;
}

relation* relFromInterm(interm_node* interm, int rel, int col, int indexOfrel, infoNode* infoMap){

  int tuple, numOftuples,i;
  uint64_t key;

  numOftuples=interm->numOfrows[indexOfrel];

  relation* r=malloc(sizeof(relation));
  if(r==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}

  r->num_tuples=(uint64_t)numOftuples;
  r->tuples=malloc(numOftuples*sizeof(tuple));
  if(r->tuples==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}

  for(i=0; i<numOftuples; i++){
    tuple=interm->rowIds[indexOfrel][i];
    key=return_value(infoMap, rel ,col, tuple);

    r->tuples[i].payload=(uint64_t)i;
    r->tuples[i].key=key;
  } 

  if(r==NULL) return NULL;

  return r; 
}



joinHistory* add_nodeHistory(int indexOfrel, joinHistory* joinHist, int numOfrels){
	int i; 
	if(indexOfrel>=numOfrels){fprintf(stderr, "Wrong indexOfrel\n" ); return NULL;}
	if(joinHist==NULL){
		joinHist=malloc(sizeof(joinHistory));
		if(joinHist==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}
		joinHist->rels=malloc(numOfrels* sizeof(char));
		if(joinHist->rels==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}
		for(i=0;i<numOfrels;i++)joinHist->rels[i]=0;
		
		joinHist->rels[indexOfrel]=1;
		joinHist->next=NULL;

	}
	else{
	
		joinHist->next=malloc(sizeof(joinHistory));
		if(joinHist->next==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}
		joinHist->next->rels=malloc(numOfrels* sizeof(char));
		if(joinHist->next->rels==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}
		for(i=0;i<numOfrels;i++)joinHist->next->rels[i]=0;
		
		joinHist->next->rels[indexOfrel]=1;
		joinHist->next->next=NULL;

	}
	
	return joinHist;
}
