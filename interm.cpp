#include "interm.h"


//apothikevei ton pinaka me ta rowIds twn apotelesmatwn sto intermediate katw apo to relation pou tou antistoixei
interm_node* store_interm_data(interm_node* interm ,uint64_t* rowIds, int indexOfrel, int numOfrows, int numOfrels){
  int i=indexOfrel;

  //printf("NUMOFROWS %d\n",numOfrows );

  interm->rowIds[i]=(uint64_t*)malloc(numOfrows* sizeof(uint64_t)); //desmevei xwro gia ta rowIds kai istera enimerwnei ta stoixeia pou xreiazetai sto interm
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
  //if(rowIds==NULL)return NULL; //ean den ipirxan koina apotelesmata epistrefei null
  if(interm==NULL){ //ean den exei arxikopoiithei akoma to intermediate, to arxikopoiw kai kataxwrw ta apotelesmata
    interm=(interm_node*)malloc(sizeof(interm_node));
    if(interm==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}
    interm->rowIds=(uint64_t**)malloc(numOfrels* sizeof(uint64_t*));
    if(interm->rowIds==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}
    for(j=0; j<numOfrels; j++)interm->rowIds[j]=NULL;
    interm->numOfrows=(int*)malloc(numOfrels* sizeof(int));
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
			if(interm->numOfrows[i]>=0)free(interm->rowIds[i]);
			
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
interm_node* self_join(interm_node* interm, infoNode* infoMap, int rel, int indexOfrel, int col1, int col2, int numOfrels, stats* rel_stats){
  int numOftuples= infoMap[rel].tuples;
  
  uint64_t* ptr1, *ptr2;
  int numOfrows;  
  uint64_t* sjoinRowIds=(uint64_t*)malloc(numOftuples* sizeof(uint64_t));
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

  //update_selfJoinStats( rel_stats,  col1,  col2 );
  
  if(sjoinRowIds!=NULL){free(sjoinRowIds); sjoinRowIds=NULL;}
  return interm; //epistrefw to intermediate enimerwmeno me ti trexousa katastash
}

void print_stats(stats* qu_stats, int rels){
  for(int i =0 ;i<rels; i++){
    printf("-----REL %d --------------------------------------\n",i );
    for(int j=0; j<qu_stats[i].columns; j++){
      printf("\tColumn: %d\n",j );
      printf("\t\tStats: u:%ld, l:%ld, f:%lf, d:%lf --\n",qu_stats[i].u[j],qu_stats[i].l[j],qu_stats[i].f[j],qu_stats[i].d[j] );
    }
    
  }
}

void update_eqStats( stats* rel_stats, int col, uint64_t val ,int found){
  uint64_t dC, fC;
  double dA,fA,new_fA;
  int i;
  rel_stats->l[col]=val;
  rel_stats->u[col]=val;
  dA=rel_stats->d[col];
  fA=rel_stats->f[col];
 
  if(found==0){
    rel_stats->d[col]=0;
    rel_stats->f[col]=0;
  }
  else if(dA==0 || fA==0){
    rel_stats->f[col]=0;
    rel_stats->d[col]=1;
  }
  else{
    printf("DIAIRW FA :%lf me dA: %lf\n",fA,dA );
    rel_stats->f[col]=fA/dA;
    rel_stats->d[col]=1;
  }
  new_fA=rel_stats->f[col];
  for(i=0; i<rel_stats->columns; i++){ 
   if(i==col)continue;
    dC=rel_stats->d[i];
    fC=rel_stats->f[i];
    if(dC==0 || fC==0|| fA==0 ||new_fA==0){ rel_stats->d[i]=0;}
    else{ rel_stats->d[i]= dC * (1 - pow( ( 1 - (new_fA / fA) ), (fC/dC) ) );}
    rel_stats->f[i]=new_fA;

  }
}

void update_gsStats(stats* rel_stats, int col, uint64_t k1, uint64_t k2){
  int i;
  double fA, new_fA, fC, dC;
  uint64_t uA, lA;

  fA=rel_stats->f[col];
  uA=rel_stats->u[col];
  lA=rel_stats->l[col];
  rel_stats->l[col]=k1;
  rel_stats->u[col]=k2;

  if((rel_stats->u[col]-rel_stats->l[col])==0){
    rel_stats->d[col]=0;
    rel_stats->f[col]=0;
    new_fA=0;
  }
  else{
    printf("old da %lf\n",rel_stats->d[col]);
    rel_stats->d[col]=((k2-k1)/(uA-lA))*rel_stats->d[col];
    printf("k2-k1= %ld\n", k2-k1);
    printf("u-l= %ld\n", uA-lA);
    printf("------------ %ld\n",(k2-k1)/((uA-lA)));
    //printf("new da %f\n",((k2-k1)/(rel_stats->u[col]-rel_stats->l[col])));
    printf("old fa %lf\n",rel_stats->f[col]);
    rel_stats->f[col]=((k2-k1)/((uA-lA)))*rel_stats->f[col];
    printf("new fa %lf\n",((k2-k1)/((uA-lA)))*rel_stats->f[col]);
    new_fA=rel_stats->f[col];
  }

  for(i=0;i<rel_stats->columns;i++){
    if(i!=col){
      fC=rel_stats->f[i];
      dC=rel_stats->d[i];
      if((fA==0)||(dC==0)){
        rel_stats->d[i]=0;
      }
      else{
        rel_stats->d[i]=dC*(1-pow((1-new_fA/fA),fC/dC));
      }
      rel_stats->f[i]=new_fA;
    }
  }
}

void update_selfJoinStats( stats* rel_stats, int col1, int col2 ){
  int i;
  uint64_t dC, fC, max, min, d ;
  double dA,fA,new_fA;

  if(rel_stats->u[col1]>rel_stats->u[col2]){ max=rel_stats->u[col1];}
  else{ max=rel_stats->u[col2];}

  if(rel_stats->l[col1]<rel_stats->l[col2]){ min=rel_stats->l[col1];}
  else{ min=rel_stats->l[col2];}

  rel_stats->u[col1]=max;
  rel_stats->u[col2]=max;

  rel_stats->l[col1]=min;
  rel_stats->l[col2]=min;

  fA=rel_stats->f[col1];
  d=max-min +1;
  if(fA==0 || d==0){
    rel_stats->f[col1]=0;
    rel_stats->f[col2]=0;
  }
  else{
    rel_stats->f[col1]=fA/d;
    rel_stats->f[col2]=fA/d;
  }
  new_fA= rel_stats->f[col1];
  dA= rel_stats->d[col1];

  if(dA==0 || fA==0 ||new_fA==0){
    rel_stats->d[col1]=0;
    rel_stats->d[col2]=0;
 }
 else{
    rel_stats->d[col1]= dA * (1 - pow( ( 1 - (new_fA / fA) ), (fA/dA) ) );
    rel_stats->d[col2]= rel_stats->d[col1];
 }

 for(i=0; i<rel_stats->columns; i++){
  if(i==col1 || i==col2)continue;
  dC=rel_stats->d[i];
  fC=rel_stats->f[i];
  if(dC==0 || fC==0|| fA==0 ||new_fA==0){ rel_stats->d[i]=0;}
  else{ rel_stats->d[i]= dC * (1 - pow( ( 1 - (new_fA / fA) ), (fC/dC) ) );}
  rel_stats->f[i]=new_fA;
 }

}


//einai h genikh synarthsh pou efarmozei filtro se relation, apofasizontas apo pou prepei
//na parei ta rowIds tou relation analoga me th katastash tou sto intermediate
interm_node* filter(interm_node* interm,int oper, infoNode* infoMap, int rel, int indexOfrel, int col, uint64_t value, int numOfrels ,stats* rel_stats){
  
  int numOftuples= (int)(infoMap[rel].tuples);  
  
  uint64_t* ptr,k1,k2;
  int numOfrows;
  int found=0;
  
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

  if(oper==3){
    if(filterRowIds!=NULL){found=1;}
    update_eqStats( rel_stats,col, value,found);
  }

  if((oper==1)||(oper==2)){
    if(oper==1){
      k1=value;
      k2=rel_stats->u[col];
      printf("k1= %ld, k2= %ld\n",k1, k2);
    }
    else{
      k1=rel_stats->l[col];
      k2=value;
      printf("k1= %ld, k2= %ld\n",k1, k2);
    }
    update_gsStats(rel_stats, col, k1, k2);
  } 

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