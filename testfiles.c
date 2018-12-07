#include "testfiles.h"

//ftiaxnei mia lista me ta onomata twn files gia na ftiaksei ton pinaka meta
RelFiles* add_Relation(RelFiles** relHead, RelFiles* relList, char* file){
  RelFiles* curr=relList;

  if(*relHead==NULL){
    curr=*relHead;
    curr=malloc(sizeof(RelFiles));
    if(curr==NULL){ fprintf(stderr, "Malloc failed \n"); return NULL;}
    strcpy(curr->file,file);
    curr->next=NULL;

    *relHead=curr;
    return curr;
  }

  if(curr->next==NULL){
    curr->next=malloc(sizeof(RelFiles));
    if(curr->next==NULL){ fprintf(stderr, "Malloc failed \n"); return NULL;}
    strcpy(curr->next->file,file);
    curr->next->next=NULL;
  }
  
  return curr->next;
}

//ektupwnei thn lista me ta onomata twn files
void print_RelFiles(RelFiles* relList){
  RelFiles* curr=relList;
  printf("Files of Relations: \n");
  while(curr!=NULL){
    printf("%s\n",curr->file );
    curr=curr->next;
  }
}

void free_RelFiles(RelFiles* relList){
	RelFiles* curr=relList, *temp;
	while(curr!=NULL){
		temp=curr;
		curr=curr->next;
		free(temp);
	}
}

//ftiaxnei thn domh pou krataei tis plhrofories 
//columns, tuples kai enan pinaka me deiktes sthn mnhmh ths prwths eggrafhs kathe sthlhs
infoNode* create_InfoMap(RelFiles* relList, infoNode* infoMap, int numOffiles){
  int i, j, fd, length;
  char *addr, *nullptr=NULL;
  uint64_t numOfcol, numOftuples;
  struct stat sb;
  RelFiles* currFile=relList;

  if(infoMap==NULL){
    infoMap=malloc(numOffiles* sizeof(infoNode));
    if(infoMap==NULL){ fprintf(stderr, "Malloc failed \n"); return NULL;}
  }

  //gia kathe file 
  for(i=0; i< numOffiles; i++){
    fd = open(currFile->file, O_RDONLY);
    if (fd==-1) {
      fprintf(stderr, "Relation file does not exist\n" );
      return NULL;
    }
    
    //megethos tou arxeiou gia thn mmap
    if (fstat(fd,&sb)==-1){fprintf(stderr, "fstat\n"); return NULL;}
    length=sb.st_size;
    
    //epistrefei deikth sthn arxh
    addr=(char*)(mmap(nullptr,length,PROT_READ,MAP_PRIVATE,fd,0u));
    if (addr==MAP_FAILED) {
       fprintf(stderr, "Cannot map \n");
        return NULL;
    }

    // oi 2 prwtoi uint64 einai oi sthles kai o arithmos twn tuples 
    //printf("%ld\n",*(uint64_t*) addr );
    numOftuples=*(uint64_t*)addr;
    addr+=sizeof(uint64_t);
    numOfcol=*(uint64_t*)addr;
    addr+=sizeof(uint64_t);
       
    infoMap[i].columns=numOfcol;
    infoMap[i].tuples=numOftuples;

    //desmeuei mnhmh gia pointers se uint64
    //den eimai sigourh an einai auto to swsto
    infoMap[i].addr=malloc(numOfcol * sizeof(uint64_t *));
    if(infoMap[i].addr==NULL){ fprintf(stderr, "Malloc failed \n"); return NULL;}
   
    //gia kathe sthlh
    for(j=0; j<numOfcol; j++){ 

      infoMap[i].addr[j]=(uint64_t)addr;
      addr+=numOftuples*sizeof(uint64_t);
      //ptr=infoMap[i].addr[j];
      //printf("First of col %ld\n", *(ptr) ); 
      
    }
    

    currFile=currFile->next;


  }
  free_RelFiles( relList);
  return infoMap;

}


void free_InfoMap(infoNode* infoMap, int numOffiles){
	for(int i=0; i< numOffiles; i++){
		free(infoMap[i].addr);

	}
	free(infoMap);
}

void print_InfoMap(infoNode* infoMap, int numOffiles){
  int i, j;
  uint64_t* ptr;
  
  for(i=0; i<numOffiles; i++){
    printf("Columns %ld Tuples %ld\n\t",infoMap[i].columns, infoMap[i].tuples );
    
    for(j=0; j<infoMap[i].columns; j++){
       ptr=(uint64_t*)infoMap[i].addr[j];
       printf("%ld| ", *ptr);
    }
    printf("\n________________________________________________\n");
   
  } 
}

interm_node* store_interm_data(interm_node* interm ,uint64_t* rowIds, int indexOfrel, int numOfrows, int numOfrels){
  int i=indexOfrel;

  //printf("NUMOFROWS %d\n",numOfrows );

  interm->rowIds[i]=malloc(numOfrows* sizeof(uint64_t));
  if(interm->rowIds[i]==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}
  memcpy(interm->rowIds[i], rowIds, numOfrows* sizeof(uint64_t));
  interm->numOfrows[i]=numOfrows;
  interm->numOfrels=numOfrels;
  return interm;
}


interm_node* update_interm(interm_node* interm, uint64_t* rowIds, int indexOfrel, int numOfrows,int numOfrels){
  int i,j;
  uint64_t* temp;
  if(rowIds==NULL)return NULL;
  if(interm==NULL){
    interm=malloc(sizeof(interm_node));
    if(interm==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}
    interm->rowIds=malloc(numOfrels* sizeof(uint64_t*));
    if(interm->rowIds==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}
    for(j=0; j<numOfrels; j++)interm->rowIds[j]=NULL;
    interm->numOfrows=malloc(numOfrels* sizeof(uint64_t));
    if(interm->numOfrows==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}
    for(j=0; j<numOfrels; j++)interm->numOfrows[j]=-1;
    interm=store_interm_data(interm,  rowIds, indexOfrel, numOfrows, numOfrels); 
    return interm;   
   
  }
  else{
    i=indexOfrel;
    if(interm->numOfrows[i]==-1){
      interm=store_interm_data(interm,  rowIds, indexOfrel, numOfrows, numOfrels); 
      return interm;
    }
    else{
      temp=interm->rowIds[i];
      free(temp);
      interm->rowIds[i]=NULL;
    
      interm=store_interm_data(interm,  rowIds, indexOfrel, numOfrows, numOfrels ); 
      return interm;
    }

  }
  
  return NULL;

}

void free_interm(interm_node* interm){
	int i,numOfrels;
	if(interm!=NULL){
		numOfrels=interm->numOfrels;
		
		for(i=0; i<numOfrels; i++){	
			if(interm->numOfrows>0)free(interm->rowIds[i]);
			
		}
		free(interm->numOfrows);
		free(interm->rowIds);
		free(interm);
	}

}

uint64_t return_value(infoNode* infoMap, int rel ,int col, int tuple){
  
  uint64_t * ptr=(uint64_t*)infoMap[rel].addr[col];
  ptr+=tuple;
  return *ptr;
}


uint64_t* filterFromInterm(interm_node* interm, int oper, uint64_t value,  int rel, int indexOfrel, int col,infoNode* infoMap, uint64_t* filterRowIds, int* numOfrows){

  int j=0, i=0;
  uint64_t tuple;
  uint64_t key;

  if(oper==3){
    for(i=0; i<interm->numOfrows[indexOfrel]; i++){
      tuple=interm->rowIds[indexOfrel][i];
      key=return_value(infoMap, rel ,col, tuple);
      if(key==value){
        filterRowIds[j]=tuple;
        //printf("Tuple %d\n",tuple );
        j++;
      }      
    }
  }
  else if(oper==2){
    //printf("gia <\n");
    for(i=0; i<interm->numOfrows[indexOfrel]; i++){
      tuple=interm->rowIds[indexOfrel][i];
      key=return_value(infoMap, rel ,col, tuple);
      if(key<value){
        filterRowIds[j]=tuple;
        //printf("Tuple %d\n",tuple );
        j++;
      }      
    }
  }
  else{
  //printf("gia >\n");
    for(i=0; i<interm->numOfrows[indexOfrel]; i++){
      tuple=interm->rowIds[indexOfrel][i];
      key=return_value(infoMap, rel ,col, tuple);
      if(key>value){
        filterRowIds[j]=tuple;
        //printf("Tuple %d\n",tuple );
        j++;
      }      
    } 
  }

  *numOfrows=j;
  if(j==0)return NULL;
  return filterRowIds;
}

uint64_t* selfjoinFromInterm(interm_node* interm, int rel, int indexOfrel, int col1, int col2, infoNode* infoMap,uint64_t* sjoinRowIds, int* numOfrows){
  int j=0, i=0;
  uint64_t tuple;
  uint64_t key1, key2;

  for(i=0; i<interm->numOfrows[indexOfrel]; i++){
    tuple=interm->rowIds[indexOfrel][i];
    key1=return_value(infoMap, rel ,col1, tuple);
    key2=return_value(infoMap, rel, col2, tuple);

    if(key1==key2){
      sjoinRowIds[j]=tuple;
      //printf("Tuple %d\n",tuple );
      j++;
    }    
  }
  *numOfrows=j;
  if(j==0)return NULL;
  return sjoinRowIds;
}

uint64_t* filterFromRel(int oper,uint64_t value,uint64_t* ptr, int numOftuples,uint64_t* filterRowIds, int* numOfrows){
  int j=0,  i=0;

  if(oper==3){
    for(i=0; i<numOftuples; i++){
      if(*ptr==value){
        filterRowIds[j]=(uint64_t)i;
        j++;
      }
      ptr++;
    }
  }
  else if(oper==2){
    //printf("gia <\n");
    for(i=0; i<numOftuples; i++){
      if(*ptr<value){
        filterRowIds[j]=(uint64_t)i;
        j++;
      }
      ptr++;
    }
  }
  else{
  //printf("gia >\n");
    for(i=0; i<numOftuples; i++){
      if(*ptr>value){
        filterRowIds[j]=(uint64_t)i;
        j++;
      }
      ptr++;
    }
  }


  *numOfrows=j;
   if(j==0){
   	free(filterRowIds);
   	filterRowIds=NULL;
   }
  return filterRowIds;
}

uint64_t* selfjoinFromRel(uint64_t* ptr1, uint64_t* ptr2, int numOftuples, uint64_t* sjoinRowIds, int* numOfrows){
  int i, j=0;
  for(i=0; i<numOftuples; i++){
    if(*ptr1==*ptr2){
      sjoinRowIds[j]=(uint64_t)i;
      j++;
    }
    ptr1++;
    ptr2++;
  }
  *numOfrows=j;
   //printf(" numofrows %d\n",j );
  if(j==0){
  	free(sjoinRowIds);
  	sjoinRowIds=NULL;
  } //den iparxoun koina stoixeia metaksi twn columns
  return sjoinRowIds;
}

interm_node* self_join(interm_node* interm, infoNode* infoMap, int rel, int indexOfrel, int col1, int col2, int numOfrels){
  int numOftuples= infoMap[rel].tuples;
  
  uint64_t* ptr1, *ptr2;
  int numOfrows;  
  uint64_t* sjoinRowIds=malloc(numOftuples* sizeof(uint64_t));
  if(sjoinRowIds==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}

  if(interm!=NULL){
    if(interm->numOfrows[indexOfrel]!=-1){     
      sjoinRowIds= selfjoinFromInterm( interm, rel, indexOfrel, col1, col2, infoMap, sjoinRowIds, &numOfrows);
    }
     else{
      ptr1=(uint64_t*)infoMap[rel].addr[col1];
      ptr2=(uint64_t*)infoMap[rel].addr[col2];
      sjoinRowIds= selfjoinFromRel( ptr1, ptr2, numOftuples, sjoinRowIds, &numOfrows);
    }
    
  }
  else{
    ptr1=(uint64_t*)infoMap[rel].addr[col1];
    ptr2=(uint64_t*)infoMap[rel].addr[col2];
    sjoinRowIds= selfjoinFromRel( ptr1, ptr2, numOftuples, sjoinRowIds, &numOfrows);  
  }
  
  interm=update_interm( interm, sjoinRowIds,  indexOfrel,  numOfrows,numOfrels);  

  if(sjoinRowIds!=NULL){free(sjoinRowIds); sjoinRowIds=NULL;}
  return interm;



}

interm_node* filter(interm_node* interm,int oper, infoNode* infoMap, int rel, int indexOfrel, int col, uint64_t value, int numOfrels){
  
  int numOftuples= (int)(infoMap[rel].tuples);
  
  
  uint64_t* ptr;
  int numOfrows;
  
  uint64_t* filterRowIds=NULL;	

  filterRowIds=(uint64_t*)malloc(numOftuples*sizeof(uint64_t)); 
  if(filterRowIds==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}
  
  if(interm!=NULL){
    if(interm->numOfrows[indexOfrel]!=-1){   

      filterRowIds= filterFromInterm( interm, oper,value, rel, indexOfrel, col, infoMap, filterRowIds, &numOfrows);
    }
     else{
      ptr=(uint64_t*)infoMap[rel].addr[col];
      filterRowIds= filterFromRel( oper,value, ptr, numOftuples, filterRowIds, &numOfrows);
    }
    
  }
  else{
  	//printf("FILTRO APO REL\n");
    ptr=(uint64_t*)infoMap[rel].addr[col];
    filterRowIds= filterFromRel( oper,value, ptr, numOftuples, filterRowIds, &numOfrows);  
  }
  
  interm=update_interm( interm, filterRowIds,  indexOfrel,  numOfrows, numOfrels);  

  if(filterRowIds!=NULL){free(filterRowIds); filterRowIds=NULL;}
  
  return interm;

}

void statusOfinterm(interm_node* interm){
  int i, j;

  if(interm!=NULL){
    for(i=0; i<interm->numOfrels; i++){
      printf(">> Rel %d with rowIds: \n",i );
     if(interm->rowIds[i]==NULL){
          printf("NULL |");
      }
      else{
      	//for(j=0; j<10; j++){
        for(j=0; j<interm->numOfrows[i]; j++){
             printf(" %ld|",interm->rowIds[i][j] );  
        }
      }
      printf("\nTotal matches: %d\n",interm->numOfrows[i] );
      printf("--------------------------------------\n");
    }
  }
}

