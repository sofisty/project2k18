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
  char *addr,*end, *nullptr=NULL;
  uint64_t numOfcol, numOftuples;
  struct stat sb;
  RelFiles* currFile=relList;
  uint64_t *ptr;

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
    if (fstat(fd,&sb)==-1){fprintf(stderr, "fstat\n"); return 1;}
    length=sb.st_size;
    
    //epistrefei deikth sthn arxh
    addr=(char*)(mmap(nullptr,length,PROT_READ,MAP_PRIVATE,fd,0u));
    if (addr==MAP_FAILED) {
       fprintf(stderr, "Cannot map \n");
        return -1;
    }

    // oi 2 prwtoi uint64 einai oi sthles kai o arithmos twn tuples 
    printf("%d\n",*(uint64_t*) addr );
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

      infoMap[i].addr[j]=(uint64_t*)addr;
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
       ptr=infoMap[i].addr[j];
       printf("%ld| ", *ptr);
    }
    printf("\n---------------------------\n");
   
  } 
}

//DEN KSERW AN EINAI O ARITHMOS SUGRISHS UINT64
int* filter(char oper, infoNode* infoMap, int rel, int col, uint64_t value){
  int numOftuples= infoMap[rel].tuples;
  uint64_t* ptr=infoMap[rel].addr[col];
  int i, j=0;
  int* filterRowIds=malloc(numOftuples* sizeof(int));
  if(filterRowIds==NULL){ fprintf(stderr, "Malloc failed \n"); return NULL;}

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

  filterRowIds[j]=-1; 
  return filterRowIds;

}