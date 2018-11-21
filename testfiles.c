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
      fprintf(stderr, "Cannot open file\n" );
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
    printf("%ld\n",*(uint64_t*) addr );
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

  return infoMap;

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

interm_node* store_interm_data(interm_node* interm ,int* rowIds, int indexOfrel, int numOfrows, int numOfrels){
  int i=indexOfrel;
  if(rowIds==NULL){
    interm->rowIds[i]=NULL;

  }
  else{
    interm->rowIds[i]=malloc(numOfrows* sizeof(int));
    if(interm->rowIds[i]==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}
    memcpy(interm->rowIds[i], rowIds, numOfrows* sizeof(int));
  }

  interm->numOfrows[i]=numOfrows;
  interm->numOfrels=numOfrels;
  return interm;
}


interm_node* update_intermFilter(interm_node* interm, int* rowIds, int indexOfrel, int numOfrows,int numOfrels){
  int i,j;
  int* temp;
  
  if(interm==NULL){
    interm=malloc(sizeof(interm_node));
    if(interm==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}
    interm->rowIds=malloc(numOfrels* sizeof(int*));
    if(interm->rowIds==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}
    for(j=0; j<numOfrels; j++)interm->rowIds[j]=NULL;
    interm->numOfrows=malloc(numOfrels* sizeof(int));
    if(interm->numOfrows==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}
    for(j=0; j<numOfrels; j++)interm->numOfrows[j]=-1;
    interm=store_interm_data(interm,  rowIds, indexOfrel, numOfrows, 1); 
    return interm;   
   
  }
  else{
    i=indexOfrel;
    if(interm->rowIds[i]==NULL){
      interm=store_interm_data(interm,  rowIds, indexOfrel, numOfrows, ((interm->numOfrels)+1) ); 
      return interm;
    }
    else{
      temp=interm->rowIds[i];
      free(temp);
      interm->rowIds[i]=NULL;
    
      interm=store_interm_data(interm,  rowIds, indexOfrel, numOfrows, interm->numOfrels ); 
      return interm;
    }

  }
  
  return NULL;

}


uint64_t return_value(infoNode* infoMap, int rel ,int col, int tuple){
  
  uint64_t * ptr=(uint64_t*)infoMap[rel].addr[col];
  ptr+=tuple;
  return *ptr;
}



int* filterFromInterm(interm_node* interm, char oper, uint64_t value,  int rel, int indexOfrel, int col,infoNode* infoMap, int* filterRowIds, int* numOfrows){

  int j=0, i=0;
  int tuple;
  uint64_t key;
  switch(oper) {
    case '=' :
      for(i=0; i<interm->numOfrows[indexOfrel]; i++){
        tuple=interm->rowIds[indexOfrel][i];
        key=return_value(infoMap, rel ,col, tuple);
        if(key==value){
          filterRowIds[j]=tuple;
          //printf("Tuple %d\n",tuple );
          j++;
         }
        
      }
     
      break;
    case '<' :
      printf("gia <\n");
      for(i=0; i<interm->numOfrows[indexOfrel]; i++){
        tuple=interm->rowIds[indexOfrel][i];
        key=return_value(infoMap, rel ,col, tuple);
        if(key<value){
          filterRowIds[j]=tuple;
          //printf("Tuple %d\n",tuple );
          j++;
         }
        
      }
      break;
    case '>' :
    printf("gia >\n");
      for(i=0; i<interm->numOfrows[indexOfrel]; i++){
        tuple=interm->rowIds[indexOfrel][i];
        key=return_value(infoMap, rel ,col, tuple);
        if(key>value){
          filterRowIds[j]=tuple;
          //printf("Tuple %d\n",tuple );
          j++;
         }
        
      }
      break;
    
  }
  *numOfrows=j;
  if(j==0)return NULL;
  return filterRowIds;
}


int* filterFromRel(char oper,uint64_t value,uint64_t* ptr, int numOftuples,int* filterRowIds, int* numOfrows){
  int j=0,  i=0;
  switch(oper) {
    case '=' :
      for(i=0; i<numOftuples; i++){
        if(*ptr==value){
          filterRowIds[j]=i;
          j++;
        }
        ptr++;
      }
      break;
    case '<' :
      printf("gia <\n");
      for(i=0; i<numOftuples; i++){
        if(*ptr<value){
          filterRowIds[j]=i;
          j++;
        }
        ptr++;
      }
      break;
    case '>' :
    printf("gia >\n");
      for(i=0; i<numOftuples; i++){
        if(*ptr>value){
          filterRowIds[j]=i;
          j++;
        }
        ptr++;
      }
      break;

  }
  *numOfrows=j;
   if(j==0)return NULL;
  return filterRowIds;
}


interm_node* filter(interm_node* interm,char oper, infoNode* infoMap, int rel, int indexOfrel, int col, uint64_t value, int numOfrels){
  
  int numOftuples= infoMap[rel].tuples;
  
  uint64_t* ptr;

  int numOfrows, posOfrel;
  
  int* filterRowIds=malloc(numOftuples* sizeof(int));
 
  if(filterRowIds==NULL){ fprintf(stderr, "Malloc failed \n"); return NULL;}
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

    ptr=(uint64_t*)infoMap[rel].addr[col];

    filterRowIds= filterFromRel( oper,value, ptr, numOftuples, filterRowIds, &numOfrows);
  
  }
  


  interm=update_intermFilter( interm, filterRowIds,  indexOfrel,  numOfrows,numOfrels);
  

  free(filterRowIds);
  return interm;

}