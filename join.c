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
