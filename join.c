#include "join.h"
#include "hash1.h"

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

joinHistory* update_nodeHistory(int indexOfrel, joinHistory* joinHist){
    if(joinHist!=NULL){
      if(joinHist->rels[indexOfrel]!=0){
        fprintf(stderr, "Already updated\n" );
        return NULL;
      }
      
      joinHist->rels[indexOfrel]=1;
      return joinHist;
    }
    else{
      fprintf(stderr, "joinHist does not exist\n" );
      return NULL;
    }

}



interm_node* join2(interm_node* interm, infoNode* infoMap, joinHistory** joinHist,int  rel1,int  indexOfrel1,int rel2,int indexOfrel2, int col1, int col2, int numOfrels){
   
  int hold, to_add,resfortest,numOfrows;
  joinHistory* currHist1, *currHist2;
  currHist1=*joinHist;
  currHist2=*joinHist;

  while(currHist1!=NULL){
    if(currHist1->rels[indexOfrel1]==1) break;
  }
  while(currHist2!=NULL){
    if(currHist2->rels[indexOfrel2]==1) break;
  }


    //if(curr1==curr2 && !NULL) eidiko selfjoin{


    //}

  if(currHist1==NULL && currHist2==NULL){
    *joinHist=add_nodeHistory(indexOfrel1,*joinHist,numOfrels);
    *joinHist=update_nodeHistory(indexOfrel2, *joinHist);
  }
    /*  
    if(curr1!=NULL && curr2==NULL) joins hold indexOfrel1 ,toadd indexOfrel2, curr_joins=curr1 
    if(curr2!=NULL && curr1==NULL) joins hold indexOfrel2   toadd indexOfrel1, curr_joins=curr2 
    if(curr1!=curr2 && !NULL){prepei na enwsoume ta nodes }

  */

  if(interm==NULL){
    relation *relation1, *relation2;
    uint64_t** rowIds;
    result* result_list=NULL;

    relation1=relFromMap(infoMap, rel1, col1);
    relation2=relFromMap(infoMap, rel2, col2);
    
    result_list=RadixHashJoin(relation1, relation2);
    if(result_list==NULL) return NULL;
    
    rowIds=resToRowIds(result_list,&numOfrows);
    interm=update_interm(interm, rowIds[0], indexOfrel1, numOfrows,numOfrels);
    interm=update_interm(interm, rowIds[1], indexOfrel2, numOfrows,numOfrels);

    statusOfinterm( interm);
    
   }
   return NULL;
 }
   /*else{
      if(interm->numOfrows[indexOfrel1]!=-1 && interm->numOfrows[indexOfrel2]!=-1){
        sunarthsh pseuto tuples apo intermediates(indexOfrel1)
        sunarthsh pseuto tuples apo intermediates(indexOfrel2)

      else if(interm->numOfrows[indexOfrel1]!=-1 && interm->numOfrows[indexOfrel2]==-1){
        sunarthsh pseuto tuples apo intermediates (indexOfrel1)
        sunarthsh tuples apo relations (indexOfrel2)
        
      }
      else if (interm->numOfrows[indexOfrel1]==-1 && interm->numOfrows[indexOfrel2]!=-1){        
        sunarthsh tuples apo relations (indexOfrel1)
        sunarthsh pseuto tuples apo intermediates (indexOfrel2)
      
      else{
        sunarthsh tuples apo relations (indexOfrel1)
        sunarthsh tuples apo relations (indexOfrel2)
      }

      radixhashjoin()
      sunarthsh apo ta results ->rowIds1 rowIds2

      if(curr_joins!=NULL){
        gia kathe sxesh pou vrisketai sto curr_joins{
          sunarthsh gia rowIds=real_rowIds(intermediate, result_rowIds, hold, indexOfrel gia update)
          interm=update_interm(indexOfrel pou dhmiourghthhke)
        }
        addjoins (relations to add , curr_joins)

        return interm;
      }
      else{
        interm=update_interm(indexOfrel1)
        interm=update_interm(indexOfrel2)

        add_indexJoins_node(indexOfrel1, indexOfrel2);
        return interm;
      }


   }*/


