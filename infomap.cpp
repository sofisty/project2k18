#include "infomap.h"

//ftiaxnei mia lista me ta onomata twn files gia na ftiaksei ton pinaka meta
RelFiles* add_Relation(RelFiles** relHead, RelFiles* relList, char* file){
  RelFiles* curr=relList; //pairnei thn lista twn files pou exoun dothei mexri twra

  if(*relHead==NULL){ //ean i lista einai adeia thn arxikopoiei me to dothen file
    curr=*relHead;
    curr=(RelFiles*)malloc(sizeof(RelFiles));
    if(curr==NULL){ fprintf(stderr, "Malloc failed \n"); return NULL;}
    strcpy(curr->file,file);
    curr->next=NULL;

    *relHead=curr;
    return curr;
  }

  if(curr->next==NULL){ //alliws prosthetei to dothen arxieo sth lista
    curr->next=(RelFiles*)malloc(sizeof(RelFiles));
    if(curr->next==NULL){ fprintf(stderr, "Malloc failed \n"); return NULL;}
    strcpy(curr->next->file,file);
    curr->next->next=NULL;
  }
  
  return curr->next; //epistrefei thn trexouda thesh pou exei ftasei i lista
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

//apodesmevei ton xwro pou exei piasei h lista me ta relation files
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
  char *addr;
  uint64_t numOfcol, numOftuples;
  struct stat sb;
  RelFiles* currFile=relList;

  if(infoMap==NULL){ //ean einai adeio to map to dimiourgei
    infoMap=(infoNode*)malloc(numOffiles* sizeof(infoNode));
    if(infoMap==NULL){ fprintf(stderr, "Malloc failed \n"); return NULL;}
  }

  //gia kathe file 
  for(i=0; i<(int)numOffiles; i++){
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
    infoMap[i].addr=(uint64_t*)malloc(numOfcol * sizeof(uint64_t ));
    if(infoMap[i].addr==NULL){ fprintf(stderr, "Malloc failed \n"); return NULL;}
   
    //gia kathe sthlh
    for(j=0; j<(int)numOfcol; j++){ 

      infoMap[i].addr[j]=(uint64_t)addr;
      addr+=numOftuples*sizeof(uint64_t);
      //ptr=infoMap[i].addr[j];
      //printf("First of col %ld\n", *(ptr) ); 
      
    }
    

    currFile=currFile->next;


  }
  free_RelFiles( relList); //apodesmevei ton xwro ths listas twn relation files
  return infoMap;

}

//apodesmevei ton xwro pou exei desmeftei gia to infoMap
void free_InfoMap(infoNode* infoMap, int numOffiles){
	for(int i=0; i< (int)numOffiles; i++){
		free(infoMap[i].addr);

	}
	free(infoMap);
}

//ektypwnei to infoMap
void print_InfoMap(infoNode* infoMap, int numOffiles){
  int i, j;
  uint64_t* ptr;
  
  for(i=0; i<(int)numOffiles; i++){
    printf("Columns %ld Tuples %ld\n\t",infoMap[i].columns, infoMap[i].tuples );
    
    for(j=0; j<(int)infoMap[i].columns; j++){
       ptr=(uint64_t*)infoMap[i].addr[j];
       printf("%ld| ", *ptr);
    }
    printf("\n________________________________________________\n");
   
  } 
}

//paei sto infoMap kai vriskei to tuple ths sthlhs tou relation pou exoun dothei kai epistrefei to key pou tou antistoixei
uint64_t return_value(infoNode* infoMap, int rel ,int col, int tuple){
  
  uint64_t * ptr=(uint64_t*)infoMap[rel].addr[col];
  ptr+=tuple;
  return *ptr;
}

//efarmozei to zitoumeno filtro pairnontas ta rowIds tou relation apo to infoMap, giati den iparxei akoma sto intermediate
uint64_t* filterFromRel(int oper,uint64_t value,uint64_t* ptr, int numOftuples,uint64_t* filterRowIds, int* numOfrows){
  int j=0,  i=0;

  if(oper==3){ //ean to filtro exei praksi isothtas
    for(i=0; i<numOftuples; i++){
      if(*ptr==value){
        filterRowIds[j]=(uint64_t)i;
        j++;
      }
      ptr++;
    }
  }
  else if(oper==2){ //ean to filtro exei praksi mikrotero
    //printf("gia <\n");
    for(i=0; i<numOftuples; i++){
      if(*ptr<value){
        filterRowIds[j]=(uint64_t)i;
        j++;
      }
      ptr++;
    }
  }
  else{ //ean to filtro exei praksi megalitero
  //printf("gia >\n");
    for(i=0; i<numOftuples; i++){
      if(*ptr>value){
        filterRowIds[j]=(uint64_t)i;
        j++;
      }
      ptr++;
    }
  }


  *numOfrows=j; //enimerwnei to posa apotelesmata vrethikan apo to filtro
   if(j==0){
   	free(filterRowIds);
   	filterRowIds=NULL;
   }
  return filterRowIds; //epistrefei ton pinaka me ta apotelesmata se rowIds
}

//efarmozei to zitoumeno join stis stiles pairnontas ta rowIds tou idiou relation apo to infoMap, giati den iparxei akoma sto intermediate
uint64_t* selfjoinFromRel(uint64_t* ptr1, uint64_t* ptr2, int numOftuples, uint64_t* sjoinRowIds, int* numOfrows){
  int i, j=0;
  for(i=0; i<numOftuples; i++){ //gia kathe ena apo ta tuples
    if(*ptr1==*ptr2){ //ean einai isa tote valta sta rowIds
      sjoinRowIds[j]=(uint64_t)i;
      j++;
    }
    ptr1++;
    ptr2++;
  }
  *numOfrows=j; //enimerwnei ton arithmo matches
   //printf(" numofrows %d\n",j );
  if(j==0){
  	free(sjoinRowIds);
  	sjoinRowIds=NULL;
  } //den iparxoun koina stoixeia metaksi twn columns
  return sjoinRowIds; //epistrefei ton pinaka twn koinwn apotelesmatwn se rowIds
}