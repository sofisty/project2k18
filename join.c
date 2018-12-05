#include "join.h"

//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
// TO CROSS MHN KSEXASOUME 
//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!


uint64_t* real_RowIds(interm_node* interm, uint64_t* rowIds, int numOfrows, int updateRel, uint64_t* newRowIds){
  int i, j; 
 //printf("UPDATE REL %d, numOfrows %d \n",updateRel, numOfrows );
 newRowIds=malloc(numOfrows* sizeof(uint64_t));
  for(i=0; i<numOfrows; i++){
    j=rowIds[i];
    newRowIds[i]=interm->rowIds[updateRel][j];
    //if(i<10)printf("prin j:%d newRowIds[i]=%ld tou %d  \n",j,newRowIds[i],updateRel);
    //printf("new tou %d=%ld\n",updateRel,newRowIds[i] );
    //printf("NEW ROW IDS %ld adn j %d \n", newRowIds[i], j);

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
    //printf("i: %d\n",i );
    r->tuples[i].key=(uint64_t)key;
  } 

  if(r==NULL) return NULL;

  return r; 
}





interm_node* special_sjoin(interm_node* interm, infoNode* infoMap, int  rel1,int  indexOfrel1,int rel2,int indexOfrel2, int col1, int col2, int numOfrels){
  int numOfrows,i=0,j=0;
  uint64_t ** rowIds, tuple1, tuple2, key1,key2;

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
  //statusOfinterm(interm);
  
  free_rowIds(rowIds);
  return interm;
}


uint64_t **  exec_join(interm_node* interm, infoNode* infoMap, int  rel1,int  indexOfrel1,int rel2,int indexOfrel2, int col1, int col2, int numOfrels, uint64_t** rowIds1, uint64_t** rowIds2, int* numOfres, int* free_flag){
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
    *free_flag=-1;
   
     
   }
   else{
   
    if(interm->numOfrows[indexOfrel1]!=-1 && interm->numOfrows[indexOfrel2]==-1){
      printf("Rel1 Interm, Rel2 Map\n");
  
      relation1=relFromInterm( interm, rel1, col1, indexOfrel1, infoMap);
      relation2=relFromMap(infoMap, rel2, col2);

     // printf("relation1 %d relation2 %d\n",relation1->num_tuples, relation2->num_tuples );
  
      //print_R(relation1);
      //print_R(relation2);

      result_list=RadixHashJoin(relation1, relation2);
      rowIds=resToRowIds( result_list, &numOfrows);
      *numOfres=numOfrows;
      printf("RETURN FROM RHJ: total matches %d\n",numOfrows);

      newRowIds1 =real_RowIds( interm, rowIds[0], numOfrows, indexOfrel1, newRowIds1);
     
      *rowIds1=newRowIds1;
      *rowIds2=rowIds[1];
      *free_flag=0;
      
      

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
      *free_flag=1;

	 

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
      
      for(int i =0; i <10; i++){
      	//printf("NEW 1 %ld NEW 2 %ld\n",newRowIds1[i], newRowIds2[i] );
      }
      *rowIds1=newRowIds1;
      *rowIds2=newRowIds2;
      *free_flag=2;

   

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
      *free_flag=-1;

     

    }

   }
 
  free_R(relation1);
  free_R(relation2);
  free_result_list( result_list);
   return rowIds;
}



joinHistory* delete_nodeHistory(int indexOfrel, joinHistory** joinHist){
  int i, numOfrels;
  joinHistory* hist=*joinHist, *prev=*joinHist, *temp;
  if(hist==NULL){fprintf(stderr, "Join Hist does not exits\n"); return NULL;}
  numOfrels=hist->numOfrels;
  if(hist->rels[indexOfrel]!=NULL){
    *joinHist=hist->next;
    temp=hist;
    
    for(i=0; i<numOfrels; i++){
    	if(temp->rels[i]!=NULL) free(temp->rels[i]);
    }
    free(temp);
    
    return *joinHist;
  }
  hist=hist->next;
  while(hist!=NULL){
    if(hist->rels[indexOfrel]!=NULL){
       prev->next=hist->next;
      temp=hist;
      
      for(i=0; i<numOfrels; i++){
    	if(temp->rels[i]!=NULL) free(temp->rels[i]);
      }
     
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

joinHistory* add_nodeHistory(int indexOfrel,int joinedRel, joinHistory* joinHist, int numOfrels){
	joinHistory* curr=joinHist;
  int i; 
 
	if(indexOfrel>=numOfrels){fprintf(stderr, "Wrong indexOfrel\n" ); return NULL;}
	if(joinHist==NULL){
		joinHist=malloc(sizeof(joinHistory));
		if(joinHist==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}
		joinHist->numOfrels=numOfrels;

		joinHist->rels=malloc(numOfrels* sizeof(int*));
		if(joinHist->rels==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}
		for(i=0;i<numOfrels;i++)joinHist->rels[i]=NULL;

		joinHist->rels[indexOfrel]=malloc(numOfrels* sizeof(int));
		if(joinHist->rels[indexOfrel]==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}
		for(i=1;i<numOfrels;i++)joinHist->rels[indexOfrel][i]=-1; //h prwth tha enhmerwthei me to joined rel
		
		joinHist->rels[indexOfrel][0]=joinedRel;
		joinHist->next=NULL;
    return joinHist;

	}
	else{
	 
    while(curr->next!=NULL){
      curr=curr->next;
    }
		curr->next=malloc(sizeof(joinHistory));
		if(curr->next==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}
		curr->next->numOfrels=numOfrels;

		curr->next->rels=malloc(numOfrels* sizeof(int*));
		if(curr->next->rels==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}
		for(i=0;i<numOfrels;i++)curr->next->rels[i]=NULL;		

		curr->next->rels[indexOfrel]=malloc(numOfrels* sizeof(int));
		if(curr->next->rels[indexOfrel]==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}
		for(i=1;i<numOfrels;i++)curr->next->rels[indexOfrel][i]=-1;
		
		
		curr->next->rels[indexOfrel][0]=joinedRel;
		curr->next->next=NULL;
    return curr->next;

	}
	
	
}

void free_joinistory(joinHistory* joinHist){
  joinHistory* curr=joinHist, *temp;
  while(curr!=NULL){
    for(int i=0; i<curr->numOfrels; i++){
      if(curr->rels[i]!=NULL){
       free(curr->rels[i]);
       curr->rels[i]=NULL;
      }

    }
    free(curr->rels);
    temp=curr;
    curr=curr->next;
    free(temp);
    temp=NULL;
  }
 
}

joinHistory* update_nodeHistory(int indexOfrel, int joinedRel, joinHistory* joinHist){
    int i=0, numOfrels;
    if(joinHist!=NULL){
      numOfrels=joinHist->numOfrels;

      if(joinHist->rels[indexOfrel]==NULL){
      	joinHist->rels[indexOfrel]=malloc(numOfrels* sizeof(int));
      	if(joinHist->rels[indexOfrel]==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}
      	for(i=1;i<numOfrels;i++)joinHist->rels[indexOfrel][i]=-1;
      	joinHist->rels[indexOfrel][0]=joinedRel;
      	return joinHist;
      }
      else{
      	while( i<numOfrels ){
	      if(joinHist->rels[indexOfrel][i]==-1) break;
	      i++;
      	}
      	if(i>=numOfrels){fprintf(stderr, "Updated joinHistory\n"); return NULL;}  
      	joinHist->rels[indexOfrel][i]=joinedRel;
      	printf("vazw to %d\n",joinedRel );
      	return joinHist;
      }
      
    }
    else{
      fprintf(stderr, "joinHist does not exist\n" );
      return NULL;
    }

}

int print_joinHist(joinHistory* joinHist){
  int numOfrels=joinHist->numOfrels, i,j;
  if(joinHist==NULL){printf("Empty joinHistory \n"); return 0;}
  printf("---Join History---: \n");
  while(joinHist!=NULL){
    for(i=0; i<numOfrels; i++){   	
      j=0;
      if(joinHist->rels[i]!=NULL){
       	printf(">Relation %d joined with : ",i );
       	while(j<numOfrels){
       		if(joinHist->rels[i][j]==-1)break;
       		else{
       			printf("%d ",joinHist->rels[i][j] );
       		}
       		j++;
      	}
      	printf("\n");
      }
    }
    printf("next node \n");
    joinHist=joinHist->next;
  }
  printf("--------------\n");
  return 0;
}

int find_join(joinHistory* joinHist, int indexOfrel1, int indexOfrel2){
	int i, numOfrels;
	if(joinHist==NULL)return 1;
	numOfrels=joinHist->numOfrels;
	if(joinHist->rels[indexOfrel1]==NULL) return 1;
	for(i=0; i<numOfrels; i++){
		if(joinHist->rels[indexOfrel1][i]==indexOfrel2) return 0;
	}
	return 1;
}

interm_node* join2(interm_node* interm, infoNode* infoMap, joinHistory** joinHist, int  rel1,int  indexOfrel1,int rel2,int indexOfrel2, int col1, int col2, int numOfrels){
   
  int hold1=-1, hold2=-1, to_add,  numOfrows=0, i, j ,free_flag;
  joinHistory* currHist1, *currHist2, *new;
  joinHistory* curr;
  uint64_t* temp, *rowIds1, *rowIds2, **updateIds;
  currHist1=*joinHist;
  currHist2=*joinHist;

  while(currHist1!=NULL){
    if(currHist1->rels[indexOfrel1]!=NULL) {hold1=indexOfrel1; break;}
    currHist1=currHist1->next;
  }
  while(currHist2!=NULL){
    if(currHist2->rels[indexOfrel2]!=NULL) { hold2=indexOfrel2; break;}
    currHist2=currHist2->next;
  }

   //statusOfinterm(interm);
 

  if((currHist1==currHist2)&&(currHist2!=NULL)&&(currHist1!=NULL)) {
    //interm=special_sjoin(interm, infoMap, rel1, indexOfrel1, rel2, indexOfrel2, col1, col2, numOfrels);  
   printf("THA MPW>>>>>>>>>>>  INDEX1 %d INDEX2 %d \n", indexOfrel1, indexOfrel2);
  	if(find_join(*joinHist, indexOfrel1, indexOfrel2)==0){
  		printf("eidiko self join\n");
  		interm=special_sjoin( interm, infoMap, rel1, indexOfrel1, rel2, indexOfrel2, col1, col2, numOfrels);
  	}
  	else{

  		updateIds=exec_join( interm, infoMap,  rel1, indexOfrel1, rel2, indexOfrel2, col1, col2,  numOfrels, &rowIds1, &rowIds2, &numOfrows, &free_flag);
  		interm= update_interm(interm, rowIds1, indexOfrel1, numOfrows,  numOfrels);
  		interm=update_interm( interm, rowIds2, indexOfrel2,  numOfrows, numOfrels);
	  	
	  	for(i=0; i<numOfrels; i++) {
	       if( (currHist1->rels[i])!=NULL && i!=indexOfrel1 && i!=indexOfrel2){ 
	        temp= real_RowIds( interm, updateIds[0], numOfrows, i, temp);
	        //for(int j=0; j<numOfrows; j++)printf(" j: %d= %ld, numOfrows %d\n",j,temp[j], numOfrows );
	        interm= update_interm(interm, temp, i, numOfrows,  numOfrels);
	         //statusOfinterm(interm);

	        free(temp);
	      }      
	    }
	   *joinHist=update_nodeHistory(indexOfrel2,indexOfrel1, *joinHist);
     *joinHist=update_nodeHistory(indexOfrel1,indexOfrel2, *joinHist);
      
      //if(rowIds1!=NULL)free(rowIds1);
      //if(rowIds2!=NULL)free(rowIds2);
     if(free_flag>-1){
      if(free_flag==0){free(rowIds1);}
      else if(free_flag==1){free(rowIds2);}
      else{free(rowIds1); free(rowIds2);}
     }
      free_rowIds(updateIds);

  	}
   
    return interm;
    
  }


  updateIds=exec_join( interm, infoMap,  rel1, indexOfrel1, rel2, indexOfrel2, col1, col2,  numOfrels, &rowIds1, &rowIds2, &numOfrows,&free_flag);
  interm= update_interm(interm, rowIds1, indexOfrel1, numOfrows,  numOfrels);
  interm=update_interm( interm, rowIds2, indexOfrel2,  numOfrows, numOfrels);

  if(currHist1==NULL && currHist2==NULL && hold1==-1 && hold2==-1){
    if(*joinHist==NULL){
       printf("mphkaa \n");
       *joinHist=add_nodeHistory(indexOfrel1,indexOfrel2,*joinHist,numOfrels);
       
       *joinHist=update_nodeHistory(indexOfrel2,indexOfrel1, *joinHist);
   
    }
    else{
      curr=add_nodeHistory(indexOfrel1,indexOfrel2, *joinHist, numOfrels);
      curr=update_nodeHistory(indexOfrel2,indexOfrel1, curr);
    
    }   
    // print_joinHist(*joinHist);
  
  //if(rowIds1!=NULL)free(rowIds1);
  //if(rowIds2!=NULL)free(rowIds2);
    if(free_flag>-1){
      if(free_flag==0){free(rowIds1);}
      else if(free_flag==1){free(rowIds2);}
      else{free(rowIds1); free(rowIds2);}
     } 
    free_rowIds(updateIds);
    return interm;
  }

  //to 1 exei kai alla sto history kai to 2 prostithetai
  if(currHist1!=NULL && currHist2==NULL) {
  
    for(i=0; i<numOfrels; i++) {
       if( (currHist1->rels[i])!=NULL && i!=indexOfrel1){
        
        temp= real_RowIds( interm, updateIds[0], numOfrows, i, temp);

        //for(int j=0; j<numOfrows; j++)printf(" j: %d= %ld, numOfrows %d\n",j,temp[j], numOfrows );
        
        interm= update_interm(interm, temp, i, numOfrows,  numOfrels);
        free(temp);
      }
      
    }
    *joinHist=update_nodeHistory(indexOfrel2,indexOfrel1, *joinHist);
     *joinHist=update_nodeHistory(indexOfrel1,indexOfrel2, *joinHist);
    // print_joinHist(*joinHist);
   
    free_rowIds(updateIds);
    //if(rowIds1!=NULL)free(rowIds1);
    //if(rowIds2!=NULL)free(rowIds2);
    if(free_flag>-1){
      if(free_flag==0){free(rowIds1);}
      else if(free_flag==1){free(rowIds2);}
      else{free(rowIds1); free(rowIds2);}
     }
    return interm;
  }
  else if(currHist1==NULL && currHist2!=NULL){
    for(i=0; i<numOfrels; i++) {
       if( (currHist2->rels[i])!=NULL && i!=indexOfrel2){
       
        temp= real_RowIds( interm, updateIds[1], numOfrows, i, temp);
        interm= update_interm(interm, temp, i, numOfrows,  numOfrels);
        free(temp);
      }
      
    }
    *joinHist=update_nodeHistory(indexOfrel1,indexOfrel2, *joinHist);
    *joinHist=update_nodeHistory(indexOfrel2,indexOfrel1, *joinHist);
     //print_joinHist(*joinHist);
  
   // if(rowIds1!=NULL)free(rowIds1);
  //if(rowIds2!=NULL)free(rowIds2);
    if(free_flag>-1){
      if(free_flag==0){free(rowIds1);}
      else if(free_flag==1){free(rowIds2);}
      else{free(rowIds1); free(rowIds2);}
     }
    free_rowIds(updateIds);
    return interm;
    
 }
 else if(currHist1!=currHist2 && currHist1!=NULL && currHist2!=NULL){

  //KANTO SUNARTHSH 
 

 //---------------------- NEW HISTORY NODE--------------------------------------------
  new=malloc(sizeof(joinHistory));
  if(new==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}
  	
  	new->numOfrels=numOfrels;
	new->rels=malloc(numOfrels* sizeof(int*));
	if(new->rels==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}
	for(i=0;i<numOfrels;i++)new->rels[i]=NULL;

	new->rels[indexOfrel1]=malloc(numOfrels* sizeof(int));
	if(new->rels[indexOfrel1]==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}
	for(i=0;i<numOfrels;i++){
		if(currHist1->rels[indexOfrel1][i]!=-1){to_add=i;}
		new->rels[indexOfrel1][i]=currHist1->rels[indexOfrel1][i]; 
		//printf(" %d \n",currHist1->rels[indexOfrel1][i] );
	
	}
	to_add++;
	if(to_add>=numOfrels){fprintf(stderr, "Variable to_add \n"); return NULL;}
	new->rels[indexOfrel1][to_add]=indexOfrel2;
		
	new->rels[indexOfrel2]=malloc(numOfrels* sizeof(int));
	if(new->rels[indexOfrel2]==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}
	for(i=0;i<numOfrels;i++){
		if(currHist2->rels[indexOfrel2][i]!=-1){to_add=i;}
		new->rels[indexOfrel2][i]=currHist2->rels[indexOfrel2][i]; 
	
	}
	to_add++;
	if(to_add>=numOfrels){fprintf(stderr, "Variable to_add \n"); return NULL;}
	new->rels[indexOfrel2][to_add]=indexOfrel1;

	new->next=NULL;
//----------------------------------------------------------------------------------

   for(i=0; i<numOfrels; i++) {
       if( (currHist1->rels[i])!=NULL && i!=indexOfrel1){
        temp= real_RowIds( interm, updateIds[0], numOfrows, i, temp);
        interm= update_interm(interm, temp, i, numOfrows,  numOfrels);
        new->rels[i]=malloc(numOfrels* sizeof(int));
        if(new->rels[i]==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}
		for(j=0;j<numOfrels;j++){
			new->rels[i][j]=currHist1->rels[i][j]; 
		}
        free(temp);
      }
      
    }
    for(i=0; i<numOfrels; i++) {
       if( (currHist2->rels[i])!=NULL && i!=indexOfrel2){
        temp= real_RowIds( interm, updateIds[1], numOfrows, i, temp);
        interm= update_interm(interm, temp, i, numOfrows,  numOfrels);
        new->rels[i]=malloc(numOfrels* sizeof(int));
        if(new->rels[i]==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}
		for(j=0;j<numOfrels;j++){
			new->rels[i][j]=currHist2->rels[i][j]; 
		}
        free(temp);
      }
      
    }
    for(i=0; i<numOfrels; i++){
    	if(new->rels[i]!=NULL){
    		for(j=0; j<numOfrels; j++){
    			//printf("new %d : %d\n",i,new->rels[i][j] );
    		}
    	}
    }

    *joinHist= merge_nodeHistory( indexOfrel1, indexOfrel2, new, joinHist);
  
   
    //if(rowIds1!=NULL)free(rowIds1);
    //if(rowIds2!=NULL)free(rowIds2);
    if(free_flag>-1){
      if(free_flag==0){free(rowIds1);}
      else if(free_flag==1){free(rowIds2);}
      else{free(rowIds1); free(rowIds2);}
     }
    free_rowIds(updateIds);
    
    return interm;
    
 }

 return NULL;

}



long long int* init_crossArr(long long int* arr, int numOfrels){
  int i;

  arr=malloc(numOfrels*(sizeof(long long int)));
  if(arr==NULL){fprintf(stderr, "Malloc failed\n"); return NULL;}

  for(i=0;i<numOfrels;i++) arr[i]=1; //to arxikopoiw me monada gia na to pollaplasiazw apla
  return arr;
}

long long int* cross_nodes(interm_node* interm,int* q_rels, infoNode* infoMap, joinHistory** joinHist, int numOfrels){
  joinHistory* currHist=*joinHist;
  long long int  rowsToMul, prev_rowsToMul; //tha to xrhsimopoihsw gia pollplassiasmo giauto arxikopoiw me 1
  int  i, j=0, k, *prevs, found,first, real_rel;

  prevs=malloc(numOfrels*sizeof(int));
  if(prevs==NULL){fprintf(stderr, "Malloc failed\n"); return NULL;}

  //h lista me to poses fores prepei na pollaplasiastei kathe relation
  long long int* toMul=NULL;
  //statusOfinterm(interm);

  //tha elegksw an h lista twn join einai keni, kai an exw diladi mono filtra
  if(*joinHist==NULL){
    for(i=0;i<numOfrels;i++){
      if(interm->numOfrows[i]!=-1){//exw filtro
        rowsToMul=interm->numOfrows[i];
        for(k=0;k<numOfrels;k++){
          if(k!=i){
            if(toMul==NULL) toMul=init_crossArr(toMul,numOfrels);
            //printf("---------------------REL :%d\n",k);
            toMul[k]=toMul[k]*rowsToMul;
            //printf("%lld-------------------rowToMul!!\n",rowsToMul);
          }
        }
      }
      else{
        real_rel=q_rels[i];
        interm->numOfrows[i]=infoMap[real_rel].tuples;
        rowsToMul=interm->numOfrows[i];
        for(k=0;k<numOfrels;k++){
          if(k!=i){
            if(toMul==NULL) toMul=init_crossArr(toMul,numOfrels);
            //printf("---------------------REL :%d\n",k);
            toMul[k]=toMul[k]*rowsToMul;
            //printf("%lld-------------------rowToMul!!\n",rowsToMul);
          }
        }
      }
    }
    statusOfCross(interm,toMul,numOfrels);  
    return toMul;
  }
  //arxika tha elegksw an iparxei kapoio filtro to opoio prepei na siberilavw
  first=1;
  for(i=0;i<numOfrels;i++){
    found=0;

    if(interm->numOfrows[i]!=-1){
      currHist=*joinHist;
      while(currHist!=NULL){
        if(currHist->rels[i]!=NULL){
          found=1;
          break;
        }
        currHist=currHist->next;
      }
      if(!found){
        //printf("REL %d has filter and %d numOfrows\n", i, interm->numOfrows[i]);
        rowsToMul=interm->numOfrows[i];
        
        for(k=0;k<numOfrels;k++){           
          if((*joinHist)->rels[k]!=NULL){
            if(first){
              prev_rowsToMul=interm->numOfrows[k];
              first=0;
            }
            if(toMul==NULL) toMul=init_crossArr(toMul,numOfrels);
            //printf("---------------------REL :%d\n",k);
            //printf("%d-------------------curr rows!\n",interm->numOfrows[k]);
            toMul[k]=toMul[k]*rowsToMul;
            //printf("%lld-------------------rowToMul!!\n",rowsToMul);
            //printf("%lld------------------after multiplication!!\n",interm->numOfrows[k]*toMul[k]);
          }
        }
        if(toMul==NULL) toMul=init_crossArr(toMul,numOfrels);
        //printf("---------------------REL :%d\n",i );
        //printf("%d-------------------curr rows!\n",interm->numOfrows[i]);
        toMul[i]=toMul[i]*prev_rowsToMul;
        //printf("%lld-------------------rowToMul!!\n",prev_rowsToMul);
        //printf("%lld------------------after multiplication!!\n",interm->numOfrows[i]*toMul[i]);

        prev_rowsToMul=prev_rowsToMul*rowsToMul;
        prevs[j]=i;
        j++;
      }
    }
  }
  currHist=*joinHist;
  //print_joinHist(*joinHist,numOfrels );
  if(currHist->next!=NULL){
    //ypologizw to cross product tou prwtou komvou joinHist kai kanw update to intermediate
    //printf("PRWTOS KOMVOS JOINHIST\n");
    first=1;
    for(i=0;i<numOfrels;i++){
      if(currHist->rels[i]!=NULL){ 
        if(first){
          prev_rowsToMul=interm->numOfrows[i];
          first=0;
        }
        //printf("REL: %d\n", i);      
        prevs[j]=i;
        j++;
      }
    }

    //twra tha ipologisw to cross product olwn twn joinhist nodes
    currHist=currHist->next;
    while(currHist!=NULL){
      //printf("NEOS KOMVOS JOINHIST\n");
      first=1;
      for(i=0;i<numOfrels;i++){
        if(currHist->rels[i]!=NULL){ 
          if(first){ //sauth th periptwsh prokeitai gia to prwto relation tou sygekrimenou joinhist node
            rowsToMul=interm->numOfrows[i];
            for(k=0;k<j;k++){
              //printf("ANANEWNW TA PREVS\n");
              if(toMul==NULL) toMul=init_crossArr(toMul,numOfrels);
              //printf("---------------------REL :%d\n",prevs[k] );
              //printf("%lld-------------------curr rows!\n",toMul[prevs[k]]*interm->numOfrows[prevs[k]]);
              toMul[prevs[k]]=toMul[prevs[k]]*rowsToMul;
              //printf("%lld-------------------rowToMul!!\n",rowsToMul);
              //printf("%lld------------------after multiplication!!\n",interm->numOfrows[prevs[k]]*toMul[prevs[k]]);
            }   
            first=0; 
          } 
         // printf("TWRA TO KAINOURGIO\n");    
          if(toMul==NULL) toMul=init_crossArr(toMul,numOfrels);
          //printf("---------------------REL :%d\n",i );
          //printf("%lld-------------------curr rows!\n",toMul[i]*interm->numOfrows[i]);
          toMul[i]=toMul[i]*prev_rowsToMul;
          //printf("%lld-------------------rowToMul!!\n",prev_rowsToMul);
          //printf("%lld------------------after multiplication!!\n",interm->numOfrows[i]*toMul[i]);

          prevs[j]=i;
          j++;   
        }
      }
      prev_rowsToMul=prev_rowsToMul*rowsToMul;
      currHist=currHist->next;
    }   
  }

  //twra tha siberilavw kai ta rows twn relations pou den exoun simmetexei se kamia praksi
  first=1;
  for(i=0;i<numOfrels;i++){
    if(interm->numOfrows[i]==-1){
      real_rel=q_rels[i];
      interm->numOfrows[i]=infoMap[real_rel].tuples;
      rowsToMul=interm->numOfrows[i];
      for(k=0;k<numOfrels;k++){
        if(k!=i){
          if(first){
            prev_rowsToMul=interm->numOfrows[k]*toMul[k];
            first=0;
          }
          //printf("---------------------REL :%d\n",k);
          toMul[k]=toMul[k]*rowsToMul;
          //printf("%lld-------------------rowToMul!!\n",rowsToMul);
        }
      }

      toMul[i]=toMul[i]*prev_rowsToMul;
    }
  }
  //ektypwnw thn lista me ta joins
  print_joinHist(*joinHist);

  //ektypwnw thn lista twn previous na dw an tairiazei me ta joins
  if(j!=0){
    printf("--crossed: ");
    for(i=0;i<j;i++){
      if(i==j-1) printf("%d ",prevs[i]);
      else printf("%d, ",prevs[i]);
    }
    printf("\n\n");
  }

  statusOfCross(interm,toMul,numOfrels);  

  free(prevs);
  return toMul;
}

void statusOfCross(interm_node* interm, long long int* toMul, int numOfrels){
  int i,j,k;

  if(toMul==NULL) printf("The cross multiplication array is empty!\n");
  else{
    for(i=0;i<numOfrels;i++){
      printf(">> Rel %d: \n",i );
      printf("\nInitial rows: %d\n",interm->numOfrows[i] );
      printf("\nTotal rowIds after cross (with duplicates): %lld\n",interm->numOfrows[i]*toMul[i] );
      printf("--------------------------------------\n");
    }
  }
}
