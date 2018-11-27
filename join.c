#include "join.h"


uint64_t* real_RowIds(interm_node* interm, uint64_t* rowIds, int numOfrows, int updateRel, uint64_t* newRowIds){
  int i, j; 
  newRowIds=malloc(numOfrows* sizeof(uint64_t));
  if(newRowIds==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}
  for(i=0; i<numOfrows; i++){
    j=rowIds[i];
    
    newRowIds[i]=interm->rowIds[updateRel][j];
    //printf("NEW ROW IDS %ld \n", newRowIds[i]);

  }

  return newRowIds;
}

relation* relFromMap(infoNode* infoMap, int rel, int col){
  int i, numOftuples;
  uint64_t* ptr;

  numOftuples= infoMap[rel].tuples;

  relation* r;
  r=malloc(sizeof(relation));
  //if(r==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}

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

  int row, numOftuples,i;
  uint64_t key;

  numOftuples=interm->numOfrows[indexOfrel];

  relation* r=malloc(sizeof(relation));
  if(r==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}

  r->num_tuples=numOftuples;

  r->tuples=malloc(numOftuples* sizeof(tuple));
  if(r->tuples==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}

  for(i=0; i<numOftuples; i++){
    row=interm->rowIds[indexOfrel][i];
    key=return_value(infoMap, rel ,col, row);
  
    
    r->tuples[i].payload=(uint64_t)i;
    r->tuples[i].key=(uint64_t)key;
  } 

  if(r==NULL) return NULL;

  return r; 
}



joinHistory* add_nodeHistory(int indexOfrel, joinHistory* joinHist, int numOfrels){
	joinHistory* curr=joinHist;
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
    return joinHist;

	}
	else{
	 
    while(curr->next!=NULL){
      curr=curr->next;
    }
		curr->next=malloc(sizeof(joinHistory));
		if(curr->next==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}
		curr->next->rels=malloc(numOfrels* sizeof(char));
		if(curr->next->rels==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}
		for(i=0;i<numOfrels;i++)curr->next->rels[i]=0;
		
		curr->next->rels[indexOfrel]=1;
		curr->next->next=NULL;
    return curr->next;

	}
	
	
}

interm_node* special_sjoin(interm_node* interm, infoNode* infoMap, int  rel1,int  indexOfrel1,int rel2,int indexOfrel2, int col1, int col2, int numOfrels){
  int numOfrows,i=0,j=0;
  uint64_t ** rowIds, *newRowIds1=NULL, *newRowIds2=NULL,tuple1, tuple2, key1,key2;

  rowIds=malloc(2*sizeof(uint64_t*));
  if(rowIds==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}

  numOfrows=interm->numOfrows[indexOfrel1];
  rowIds[0]=malloc(numOfrows*sizeof(uint64_t));
  if(rowIds[0]==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}
  rowIds[1]=malloc(numOfrows*sizeof(uint64_t));
  if(rowIds[1]==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}


  printf("Rel1 Interm, Rel2 Interm\n");
  
  for(i=0; i<numOfrows; i++){
    tuple1=interm->rowIds[indexOfrel1][i];
    tuple2=interm->rowIds[indexOfrel2][i];
    key1=return_value(infoMap, rel1 ,col1, tuple1);
    key2=return_value(infoMap, rel2, col2, tuple2);

    if(key1==key2){
      rowIds[0][j]=tuple1;
      rowIds[1][j]=tuple2;
      j++;
    }    
  }

  printf("special_sjoin: total matches %d\n",j);

  interm=update_interm( interm, rowIds[0], indexOfrel1,  j, numOfrels);
  interm= update_interm( interm, rowIds[1], indexOfrel2, j,  numOfrels);
  statusOfinterm(interm);
  return interm;
}


uint64_t **  exec_join(interm_node* interm, infoNode* infoMap, int  rel1,int  indexOfrel1,int rel2,int indexOfrel2, int col1, int col2, int numOfrels, uint64_t** rowIds1, uint64_t** rowIds2, int* numOfres){
  int numOfrows;
  uint64_t ** rowIds, *newRowIds1=NULL, *newRowIds2=NULL;
  relation *relation1=NULL, *relation2=NULL;
  result* result_list=NULL;
  if(interm==NULL){
    printf("CREATE INTERM: Rel1 Map, Rel2 Map\n");
  
    relation1=relFromMap(infoMap, rel1, col1);
    relation2=relFromMap(infoMap, rel2, col2);
    

    result_list=RadixHashJoin(relation1, relation2);
    print_results(result_list, &numOfrows);
    //print_results(result_list, &resfortest);
    

    rowIds=resToRowIds( result_list, &numOfrows);
    *numOfres=numOfrows;
    printf("total matches %d\n",numOfrows );
    
    *rowIds1=rowIds[0];
    *rowIds2= rowIds[1];

    free_R(relation1);
    free_R(relation2);
     
   }
   else{
   
    if(interm->numOfrows[indexOfrel1]!=-1 && interm->numOfrows[indexOfrel2]==-1){
      printf("Rel1 Interm, Rel2 Map\n");
  
      relation1=relFromInterm( interm, rel1, col1, indexOfrel1, infoMap);
      relation2=relFromMap(infoMap, rel2, col2);

      printf("relation1 %d relation2 %d\n",relation1->num_tuples, relation2->num_tuples );
  
      //print_R(relation1);
      //print_R(relation2);

      result_list=RadixHashJoin(relation1, relation2);
      rowIds=resToRowIds( result_list, &numOfrows);
      *numOfres=numOfrows;
      printf("RETURN FROM RHJ: total matches %d\n",numOfrows);

      newRowIds1 =real_RowIds( interm, rowIds[0], numOfrows, indexOfrel1, newRowIds1);
     
      *rowIds1=newRowIds1;
      *rowIds2=rowIds[1];

      rowIds[0]=*rowIds1;
      free_R(relation2);

    }
    else if(interm->numOfrows[indexOfrel1]==-1 && interm->numOfrows[indexOfrel2]!=-1){
      printf("Rel1 Map, Rel2 Interm\n");
     
      relation1=relFromMap(infoMap, rel1, col1);
      relation2=relFromInterm( interm, rel2, col2, indexOfrel2, infoMap);

      result_list=RadixHashJoin(relation1, relation2);
      rowIds=resToRowIds( result_list, &numOfrows);
      *numOfres=numOfrows;
      printf("RETURN FROM RHJ: total matches %d\n",numOfrows);

      newRowIds2 =real_RowIds( interm, rowIds[1], numOfrows, indexOfrel2, newRowIds2);
      

      *rowIds2=newRowIds2;
      *rowIds1=rowIds[0];

      rowIds[1]=*rowIds2;

	 free_R(relation1);

    }
    else if(interm->numOfrows[indexOfrel1]!=-1 && interm->numOfrows[indexOfrel2]!=-1){
      printf("Rel1 Interm, Rel2 Interm\n");

      relation1=relFromInterm( interm, rel1, col1, indexOfrel1, infoMap);
      relation2=relFromInterm( interm, rel2, col2, indexOfrel2, infoMap);

      result_list=RadixHashJoin(relation1, relation2);
      rowIds=resToRowIds( result_list, &numOfrows);
      *numOfres=numOfrows;
      printf("RETURN FROM RHJ: total matches %d\n",numOfrows);

      newRowIds1 =real_RowIds( interm, rowIds[0], numOfrows, indexOfrel1, newRowIds1);
      newRowIds2 =real_RowIds( interm, rowIds[1], numOfrows, indexOfrel2, newRowIds2);
      
      *rowIds1=newRowIds1;
      *rowIds2=newRowIds2;

      rowIds[0]=*rowIds1;
      rowIds[1]=*rowIds2;


    }
    else if(interm->numOfrows[indexOfrel1]==-1 && interm->numOfrows[indexOfrel2]==-1){ //uparxei to intermediate alla kanena relation apo auta
       printf("Rel1 Map, Rel2 Map\n");
      relation1=relFromMap(infoMap, rel1, col1);
      relation2=relFromMap(infoMap, rel2, col2);
      

      result_list=RadixHashJoin(relation1, relation2);
      //print_results(result_list, &resfortest);
      rowIds=resToRowIds( result_list, &numOfrows);
      *numOfres=numOfrows;
      printf("total matches %d\n",numOfrows );

      *rowIds1=rowIds[0];
      *rowIds2=rowIds[1];

      free_R(relation1);
      free_R(relation2);

    }

   }
   free_result_list( result_list);
   return rowIds;
}

joinHistory* delete_nodeHistory(int indexOfrel, joinHistory** joinHist){
  joinHistory* hist=*joinHist, *prev=*joinHist, *temp;
  if(hist->rels[indexOfrel]==1){
    temp=hist;
    *joinHist=hist->next;
    free(temp);
    return *joinHist;
  }
  hist=hist->next;
  while(hist!=NULL){
    if(hist->rels[indexOfrel]==1){
      temp=hist;
      prev->next=hist->next;
      free(temp);
      return *joinHist;
    }
    prev=hist;
    hist=hist->next;
  }

  fprintf(stderr, "Relation %d does not exist in JoinHIstory list\n",indexOfrel );
  return NULL;


}

joinHistory* merge_nodeHistory(int indexOfrel1, int indexOfrel2, joinHistory* new, joinHistory** joinHist){
  joinHistory* temp;
  *joinHist= delete_nodeHistory( indexOfrel1, joinHist);

  
  *joinHist=delete_nodeHistory(indexOfrel2, joinHist);

  
  if(*joinHist==NULL){
    *joinHist=new;
    return *joinHist;
  }
  else{
    temp=*joinHist;
    *joinHist=new;
    (*joinHist)->next=temp;
    return *joinHist;
  }

  
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

int print_joinHist(joinHistory* joinHist, int numOfrels){
  
  if(joinHist==NULL){printf("Empty joinHistory \n"); return 0;}
  
  printf("Joined relations: \n");
  while(joinHist!=NULL){
    printf("\t");
    for(int i =0 ;i<numOfrels; i++){
      if(joinHist->rels[i]==1){printf(" %d,",i);}
    }
    printf("\n");
    joinHist=joinHist->next;
  }
  return 0;
}


interm_node* join2(interm_node* interm, infoNode* infoMap, joinHistory** joinHist, int  rel1,int  indexOfrel1,int rel2,int indexOfrel2, int col1, int col2, int numOfrels){
   
  int hold1=-1, hold2=-1, to_add1, to_add2, resfortest, numOfrows=0, i;
  joinHistory* currHist1, *currHist2, *new;
  joinHistory* curr;
  uint64_t* temp, *rowIds1, *rowIds2, **updateIds;
  currHist1=*joinHist;
  currHist2=*joinHist;

  while(currHist1!=NULL){
    if(currHist1->rels[indexOfrel1]==1) {hold1=indexOfrel1; break;}
    currHist1=currHist1->next;
  }
  while(currHist2!=NULL){
    if(currHist2->rels[indexOfrel2]==1) { hold2=indexOfrel2; break;}
    currHist2=currHist2->next;
  }


  updateIds=exec_join( interm, infoMap,  rel1, indexOfrel1, rel2, indexOfrel2, col1, col2,  numOfrels, &rowIds1, &rowIds2, &numOfrows);
  interm= update_interm(interm, rowIds1, indexOfrel1, numOfrows,  numOfrels);
  interm=update_interm( interm, rowIds2, indexOfrel2,  numOfrows, numOfrels);
   //statusOfinterm(interm);


  if((currHist1==currHist2)&&(currHist2!=NULL)&&(currHist1!=NULL)) {
    //interm=special_sjoin(interm, infoMap, rel1, indexOfrel1, rel2, indexOfrel2, col1, col2, numOfrels);
    for(i=0; i<numOfrels; i++) {
       if( (currHist1->rels[i])==1 && i!=indexOfrel1 && i!=indexOfrel2){
        
        temp= real_RowIds( interm, updateIds[0], numOfrows, i, temp);
        //for(int j=0; j<numOfrows; j++)printf(" j: %d= %ld, numOfrows %d\n",j,temp[j], numOfrows );
        interm= update_interm(interm, temp, i, numOfrows,  numOfrels);
      }      
    }
    return interm;
  }
  else if(currHist1==NULL && currHist2==NULL && hold1==-1 && hold2==-1){
    if(*joinHist==NULL){
       *joinHist=add_nodeHistory(indexOfrel1,*joinHist,numOfrels);
       *joinHist=update_nodeHistory(indexOfrel2, *joinHist);
        
    }
    else{
      curr=add_nodeHistory(indexOfrel1, *joinHist, numOfrels);
      curr=update_nodeHistory(indexOfrel2, curr);
    
    }   
    return interm;
  }

  //to 1 exei kai alla sto history kai to 2 prostithetai
  if(currHist1!=NULL && currHist2==NULL) {
   
    for(i=0; i<numOfrels; i++) {
       if( (currHist1->rels[i])==1 && i!=indexOfrel1){
        
        temp= real_RowIds( interm, updateIds[0], numOfrows, i, temp);

        //for(int j=0; j<numOfrows; j++)printf(" j: %d= %ld, numOfrows %d\n",j,temp[j], numOfrows );
        interm= update_interm(interm, temp, i, numOfrows,  numOfrels);
      }
      
    }
    *joinHist=update_nodeHistory(indexOfrel2, *joinHist);
    return interm;
  }
  else if(currHist1==NULL && currHist2!=NULL){
    for(i=0; i<numOfrels; i++) {
       if( (currHist2->rels[i])==1 && i!=indexOfrel2){
        temp= real_RowIds( interm, updateIds[1], numOfrows, i, temp);
        interm= update_interm(interm, temp, i, numOfrows,  numOfrels);
      }
      
    }
    *joinHist=update_nodeHistory(indexOfrel1, *joinHist);
    return interm;
    
 }
 else if(currHist1!=currHist2 && currHist1!=NULL && currHist2!=NULL){

  //KANTO SUNARTHSH 
  new=malloc(sizeof(joinHistory));
  if(new==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}
  new->rels=malloc(numOfrels* sizeof(char));
  if(new->rels==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}
  for(i=0;i<numOfrels;i++)new->rels[i]=0;
  new->next=NULL;
  //------------------
  new->rels[indexOfrel1]=1;
  new->rels[indexOfrel2]=1;

   for(i=0; i<numOfrels; i++) {
       if( (currHist1->rels[i])==1 && i!=indexOfrel1){
        temp= real_RowIds( interm, updateIds[0], numOfrows, i, temp);
        interm= update_interm(interm, temp, i, numOfrows,  numOfrels);
        new->rels[i]=1;
      }
      
    }
    for(i=0; i<numOfrels; i++) {
       if( (currHist2->rels[i])==1 && i!=indexOfrel2){
        temp= real_RowIds( interm, updateIds[1], numOfrows, i, temp);
        interm= update_interm(interm, temp, i, numOfrows,  numOfrels);
        new->rels[i]=1;
      }
      
    }

    *joinHist= merge_nodeHistory( indexOfrel1, indexOfrel2, new, joinHist);
  
    return interm;
 }



  }


/*
interm_node* join2(interm_node* interm, infoNode* infoMap, joinHistory** joinHist,int rel1,int indexOfrel1,int rel2,int indexOfrel2, int col1, int col2, int numOfrels){
    joinHistory *currHist1=*joinHist, *currHist2=*joinHist;
    int hold, to_add1,to_add2, resfortest=0; 
    relation *relation1, *relation2;
    result* result_list=NULL;
  
      
      while(currHist1!=NULL){
        if(currHist1->rels[indexOfrel1]==1) break;
      }

      while(currHist2!=NULL)
        if(currHist2->rels[indexOfrel2]==1) break;


       if(currHist1=NULL && currHist2==NULL){
          *joinHist=add_nodeHistory( indexOfrel1, *joinHist, numOfrels);
          *joinHist= update_nodeHistory( indexOfrel2, *joinHist);


      } 
      /*if(curr1==curr2 && !NULL) eidiko selfjoin{


      }
      if(curr1!=NULL && curr2==NULL) joins hold indexOfrel1 ,toadd indexOfrel2, curr_joins=curr1 
      if(curr2!=NULL && curr1==NULL) joins hold indexOfrel2   toadd indexOfrel1, curr_joins=curr2 
      if(curr1!=curr2 && !NULL){prepei na enwsoume ta nodes }
    */
 /*
    if(interm==NULL){
        relation1=relFromMap(infoMap, rel1, col1);
        relation2=relFromMap(infoMap, rel2, col2);

        result_list=RadixHashJoin(relation1, relation2);
        print_results(result_list, &resfortest);
        
        return NULL;

     // gia kathe relation kalese thn updatefilter() {mallon tha einai h idia kai tha kanei update ta intermedates gia 1 relation}
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


     }
    */




//}