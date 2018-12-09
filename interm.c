#include "interm.h"

//apothikevei ton pinaka me ta rowIds twn apotelesmatwn sto intermediate katw apo to relation pou tou antistoixei
interm_node* store_interm_data(interm_node* interm ,uint64_t* rowIds, int indexOfrel, int numOfrows, int numOfrels){
  int i=indexOfrel;

  //printf("NUMOFROWS %d\n",numOfrows );

  interm->rowIds[i]=malloc(numOfrows* sizeof(uint64_t)); //desmevei xwro gia ta rowIds kai istera enimerwnei ta stoixeia pou xreiazetai sto interm
  if(interm->rowIds[i]==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}
  memcpy(interm->rowIds[i], rowIds, numOfrows* sizeof(uint64_t));
  interm->numOfrows[i]=numOfrows;
  interm->numOfrels=numOfrels;
  return interm;
}

//enimerwnei to intermediate me ta kainourgia rowIds pou proekipsan ws apotelesmata kapoiou join/filtrou
interm_node* update_interm(interm_node* interm, uint64_t* rowIds, int indexOfrel, int numOfrows,int numOfrels){
  int i,j;
  uint64_t* temp;
  if(rowIds==NULL)return NULL; //ean den ipirxan koina apotelesmata epistrefei null
  if(interm==NULL){ //ean den exei arxikopoiithei akoma to intermediate, to arxikopoiw kai kataxwrw ta apotelesmata
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
  else{ //alliws apla kataxwrw ta apotelesmata 
    i=indexOfrel;
    if(interm->numOfrows[i]==-1){ //ean den exei ksanaxrisimopoiithei to dothen relation se kapoia praksi, tote apla apothikevw ta rowIds
      interm=store_interm_data(interm,  rowIds, indexOfrel, numOfrows, numOfrels); 
      return interm;
    }
    else{ //alliws , svinw prwta ta apotelesmata pou iparxoun kai meta apothikevw ta kainourgia koina apotelesmata
      temp=interm->rowIds[i];
      free(temp);
      interm->rowIds[i]=NULL;
    
      interm=store_interm_data(interm,  rowIds, indexOfrel, numOfrows, numOfrels ); 
      return interm; //epitrefw to intermediate
    }

  }
  
  return NULL;

}

//apodesmevei ton xwro pou exei desmeftei gia to intermediate
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

//efarmozei to zitoumeno filtro pairnontas ta iparxonta rowIds tou relation apo to intermediate, kathws auto exei ksanasimmetasxei 
//se praksi
uint64_t* filterFromInterm(interm_node* interm, int oper, uint64_t value,  int rel, int indexOfrel, int col,infoNode* infoMap, uint64_t* filterRowIds, int* numOfrows){

  int j=0, i=0;
  uint64_t tuple;
  uint64_t key;

  if(oper==3){ //ean prokeitai gia thn praksi isothtas
    for(i=0; i<interm->numOfrows[indexOfrel]; i++){
      tuple=interm->rowIds[indexOfrel][i];
      key=return_value(infoMap, rel ,col, tuple);
      if(key==value){
        filterRowIds[j]=tuple;
        j++;
      }      
    }
  }
  else if(oper==2){//alliws ean prokeitai gia thn praksi mikrotero
    for(i=0; i<interm->numOfrows[indexOfrel]; i++){
      tuple=interm->rowIds[indexOfrel][i];
      key=return_value(infoMap, rel ,col, tuple);
      if(key<value){
        filterRowIds[j]=tuple;
        j++;
      }      
    }
  }
  else{ //alliws ean proketai gia thn praksi mikrotero
    for(i=0; i<interm->numOfrows[indexOfrel]; i++){
      tuple=interm->rowIds[indexOfrel][i];
      key=return_value(infoMap, rel ,col, tuple);
      if(key>value){
        filterRowIds[j]=tuple;
        j++;
      }      
    } 
  }

  *numOfrows=j; //enimerwnei ton aritmo koinwn apotelesmatwn
  if(j==0)return NULL;
  return filterRowIds; //epistrefei ton pinaka rowIds me ta apotelesmata tou filtrou
}

//efarmozei to zitoume join sta columns enos relation, pairnontas omws ta rowIds tou apo to intermediate kathws auto exei ksanasimmetasxei
//se kapoia praksi prin 
uint64_t* selfjoinFromInterm(interm_node* interm, int rel, int indexOfrel, int col1, int col2, infoNode* infoMap,uint64_t* sjoinRowIds, int* numOfrows){
  int j=0, i=0;
  uint64_t tuple;
  uint64_t key1, key2;

  for(i=0; i<interm->numOfrows[indexOfrel]; i++){ //gia kathe tuple pou vrisketai sta rowIds tou interm gia to relation
    tuple=interm->rowIds[indexOfrel][i];
    key1=return_value(infoMap, rel ,col1, tuple);
    key2=return_value(infoMap, rel, col2, tuple);

    if(key1==key2){ //ean einai isa tote to prosthetei ston pinaka rowIds koinwn apotelesmatwn
      sjoinRowIds[j]=tuple;
      j++;
    }    
  }
  *numOfrows=j; //enimerwnei ton arithmo koinwn apotelesmatwn
  if(j==0)return NULL;
  return sjoinRowIds; //epistrefei ton pinaka rowIds koinwn apotelesmatwn
}

//einai h genikh synarthsh pou efarmozei selfjoin se diaforetikes sthles koinou relation, apofasizontas apo pou prepei
//na parei ta rowIds tou relation analoga me th katastash tou sto intermediate
interm_node* self_join(interm_node* interm, infoNode* infoMap, int rel, int indexOfrel, int col1, int col2, int numOfrels){
  int numOftuples= infoMap[rel].tuples;
  
  uint64_t* ptr1, *ptr2;
  int numOfrows;  
  uint64_t* sjoinRowIds=malloc(numOftuples* sizeof(uint64_t));
  if(sjoinRowIds==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}

  if(interm!=NULL){ //ean exei arxikopoihthei to intermediate, DEN prokeitai diladi gia thn prwth praksi tou query
    if(interm->numOfrows[indexOfrel]!=-1){ //ean exei ksanasimmetexei kapou to rel pairnw ta rowIds apo to interm kai kanw to selfjoin
      sjoinRowIds= selfjoinFromInterm( interm, rel, indexOfrel, col1, col2, infoMap, sjoinRowIds, &numOfrows);
    }
     else{ //alliws parinw ta rowIds apo to infoMap, kai kanw to selfjoin
      ptr1=(uint64_t*)infoMap[rel].addr[col1];
      ptr2=(uint64_t*)infoMap[rel].addr[col2];
      sjoinRowIds= selfjoinFromRel( ptr1, ptr2, numOftuples, sjoinRowIds, &numOfrows);
    }
    
  }
  else{ //ean alliws prokeitai gia tthn prwth praksi, kanw to selfjoin me ta rowIds tou infomap
    ptr1=(uint64_t*)infoMap[rel].addr[col1];
    ptr2=(uint64_t*)infoMap[rel].addr[col2];
    sjoinRowIds= selfjoinFromRel( ptr1, ptr2, numOftuples, sjoinRowIds, &numOfrows);  
  }
  
  //enimerwnw to intermediate me ta kainourgia oina apotelesmata
  interm=update_interm( interm, sjoinRowIds,  indexOfrel,  numOfrows,numOfrels);  

  if(sjoinRowIds!=NULL){free(sjoinRowIds); sjoinRowIds=NULL;}
  return interm; //epistrefw to intermediate enimerwmeno me ti trexousa katastash
}

//einai h genikh synarthsh pou efarmozei filtro se relation, apofasizontas apo pou prepei
//na parei ta rowIds tou relation analoga me th katastash tou sto intermediate
interm_node* filter(interm_node* interm,int oper, infoNode* infoMap, int rel, int indexOfrel, int col, uint64_t value, int numOfrels){
  
  int numOftuples= (int)(infoMap[rel].tuples);  
  
  uint64_t* ptr;
  int numOfrows;
  
  uint64_t* filterRowIds=NULL;	

  filterRowIds=(uint64_t*)malloc(numOftuples*sizeof(uint64_t)); 
  if(filterRowIds==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}
  
  if(interm!=NULL){ //ean to intermediate den einai keno, diladi den prokeitai gia tin prwth praksi
    if(interm->numOfrows[indexOfrel]!=-1){  //ean to relation exei ksanaxrisimopoiithei se alli praksi pairnw apo to interm ta rowIds kai efarmozw to filtro 

      filterRowIds= filterFromInterm( interm, oper,value, rel, indexOfrel, col, infoMap, filterRowIds, &numOfrows);
    }
     else{ //alliws pairnw apo to infoma ta rowIds kai efarmozw to filtro
      ptr=(uint64_t*)infoMap[rel].addr[col];
      filterRowIds= filterFromRel( oper,value, ptr, numOftuples, filterRowIds, &numOfrows);
    }
    
  }
  else{ //alliws ean einai h prwth praksi, pairnw apo to infomap ta rowIds kai efarmozw to filtro
    ptr=(uint64_t*)infoMap[rel].addr[col];
    filterRowIds= filterFromRel( oper,value, ptr, numOftuples, filterRowIds, &numOfrows);  
  }
  
  //emimerwnei to intermediate me ta apotelesmata tou filtrou gia to dothen relation
  interm=update_interm( interm, filterRowIds,  indexOfrel,  numOfrows, numOfrels);  

  if(filterRowIds!=NULL){free(filterRowIds); filterRowIds=NULL;}
  
  return interm;//epistrefei to intermediate sth trexousa katastash enimerwmeno

}

//ektipwnei thn trexousa katastash tou intermediate
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