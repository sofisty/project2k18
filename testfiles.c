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


//##########  GIA ARGOTERA  ##############
//THEWRW OTI APOTHHKEYONTAI TA PRAGMATIKA RELATIONS
//TOY STYL OTI OTAN TO FILTRO LEEI 0.1>100 1.2<10.000 KAI TA RELATIONS EINAI TO 1 4
//TO INTERMEDIATE KAI TO FILTER PAIRNOUN TO 1 KAI 4 


//kanei update gia 1 relation
interm_node* store_interm_data(interm_node* interm, int posOfrel, int numOfrows, int rel, int* rowIds,int numOfrels){
  interm->rels[posOfrel]=rel;
  interm->numOfrels=numOfrels;
  interm->numOfrows=numOfrows;
  interm->rowIds=malloc(sizeof(int*));
  if(interm->rowIds==NULL){ fprintf(stderr, "Malloc failed \n"); return NULL;}
  //twra apla exei mono ena int*
  
  if(rowIds==NULL){ //ama den exei apotelesmata
      interm->rowIds[0]=NULL;
  }
  else{
    interm->rowIds[0]=malloc(numOfrows* sizeof(int));
    if(interm->rowIds[0]==NULL){ fprintf(stderr, "Malloc failed \n"); return NULL;}
    //epeidh to rowIds pou pairnei san orisma einai enas pinakas osa tuples exei to relation
    //edw antigrafontai mono oi numOfrows grammes, dhladh oi theseis pou krataei ta apotelesmata kai oxi ta upoloipa poy einai skoupidia
    memcpy(interm->rowIds[0], rowIds, numOfrows * sizeof(int));   
  } 
  return interm;

}


interm_node* update_intermFilter(interm_node** interm_header, int* rowIds, int rel, int numOfrows, interm_node* new){
  int  flag=0;
  interm_node* interm=*interm_header;
  interm_node* curr, *prev;

  //H prwth eisagwgh node, dhmiourgia tou header
  if(*interm_header==NULL){
   
    new->rels= malloc(sizeof(int));
    //den kserw an xreiazetai to malloc
    if(new->rels==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}
    
    new= store_interm_data(new, 0, numOfrows, rel, rowIds, 1);
    new->next=NULL;
    
    *interm_header=new;
   
    return *interm_header;    
   
  }
  //An uparxei hdh ena node
  else{
    curr=*interm_header;
    
    //1. eksetazoume an uparxei hdh to relation 
    //   pou shmaine oti prepei na to diagrapsoume kai na valoume to kainourgio sthn swsth thesh
    
    //PRWTA GIA TON HEADER    
    if(interm->rels[0]==rel){
     // printf("VRHKA IDIO RELATION UPDATE HEADER\n");
     
      new->next=(*interm_header)->next;
      new->rels= malloc(sizeof(int));
      if(new->rels==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}
      new =store_interm_data(new, 0, numOfrows, rel, rowIds,1);
        
      *interm_header=new;

      //svhnoume to palio
      free_intermNode(curr);
      
      printf("------------------------------------\n");
      print_interm(*interm_header);
      return *interm_header;

    }
    //psaxnoume an uparxei to relation sta upoloipa nodes
    else{
      // an uparxei to relation se kapoia allh thesh to svhnoume   
      prev=interm;
      curr=interm;
      while(curr!=NULL){
        if(curr->rels[0]==rel){
          flag=1;
          break;
        }
        prev=curr;
        curr=curr->next;
      }
      if(flag==1){
        prev->next=curr->next;
        free_intermNode(curr);
      }

      //eksetazoume an prepei na mpei prin apo ton head
      if(interm->numOfrows>numOfrows){
     
       //printf("HEADER > NUMOFROWS\n");
      
        new->next=interm;
        new->rels= malloc(sizeof(int));
        if(new->rels==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}
        new =store_interm_data(new, 0, numOfrows, rel, rowIds,1);
      
        *interm_header=new;
       
        printf("------------------------------------\n");
        print_interm(*interm_header);

        return *interm_header;
       }
      
      //anazhtame sta upoloipa nodes thn thesh tou me vash to numOfrows 
      
      curr=interm->next;
      prev=interm;
      while(curr!=NULL){
        if(curr->numOfrows>numOfrows)break;
        prev=curr;
        curr=curr->next;
      }
     
      new->rels= malloc(sizeof(int));
      if(new->rels==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}
      new =store_interm_data(new, 0, numOfrows, rel, rowIds,1);

      new->next=curr;
      prev->next=new;

      printf("------------------------------------\n");
      print_interm(*interm_header);

      return *interm_header;
   
    }   
    

  }
  
  return NULL;

}

void print_interm(interm_node* interm){
  interm_node* curr=interm;
  while(curr!=NULL){
    printf("-%d\n",curr->numOfrows );
    curr=curr->next;
  }

}

void free_intermNode(interm_node* interm){
  free(interm->rels);
  if(interm->rowIds!=NULL){
    free(interm->rowIds);
  }
  free(interm);
}

interm_node* search_interm(int rel, interm_node* interm, int* posOfrel){
  int i;
  while(interm!=NULL){
    for(i=0;i<interm->numOfrels; i++){
      if(rel==interm->rels[i]){
        *posOfrel=i;
         return interm;
      }

     
    }
   
    interm=interm->next;
  }

  return NULL;
}

uint64_t return_value(infoNode* infoMap, int rel ,int col, int tuple){
  
  uint64_t * ptr=(uint64_t*)infoMap[rel].addr[col];
  ptr+=tuple;
  return *ptr;
}

int* filterFromInterm(interm_node* curr_interm, char oper, uint64_t value,  int rel, int col,infoNode* infoMap, int* filterRowIds, int* numOfrows){
  int j=0, i=0;
  int tuple;
  uint64_t key;
  switch(oper) {
    case '=' :
      for(i=0; i<curr_interm->numOfrows; i++){
        tuple=curr_interm->rowIds[0][i];
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
      for(i=0; i<curr_interm->numOfrows; i++){
        tuple=curr_interm->rowIds[0][i];
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
      for(i=0; i<curr_interm->numOfrows; i++){
        tuple=curr_interm->rowIds[0][i];
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
  printf(" numofrows %d\n",j );
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
   printf(" numofrows %d\n",j );
   if(j==0)return NULL;
  return filterRowIds;
}


interm_node* filter(interm_node* interm,char oper, infoNode* infoMap, int rel, int col, uint64_t value){
  interm_node* curr_interm;
  interm_node* new;
  
  int numOftuples= infoMap[rel].tuples;
  
  uint64_t* ptr;

  int numOfrows, posOfrel;
  
  int* filterRowIds=malloc(numOftuples* sizeof(int));
 
  if(filterRowIds==NULL){ fprintf(stderr, "Malloc failed \n"); return NULL;}
  if(interm!=NULL){
    curr_interm=search_interm(rel, interm, &posOfrel);
    if(curr_interm!=NULL){
     
      filterRowIds= filterFromInterm( curr_interm, oper,value, rel, col, infoMap, filterRowIds, &numOfrows);

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
  


  new=malloc(sizeof(interm_node));
  if(new==NULL){ fprintf(stderr, "Malloc failed \n"); return NULL;}
  interm=update_intermFilter( &interm, filterRowIds, rel, numOfrows, new); 

  free(filterRowIds);
  return interm;

}