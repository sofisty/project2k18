#include "join.h"

//metatrepei ta row ids pou epistrefei h result_list apo to rhj sta pragmatika me vash ta relations
//epistrefei ta pragmatika rowIds ws uint64_t*
uint64_t* real_RowIds(interm_node* interm, uint64_t* rowIds, int numOfrows, int updateRel, uint64_t* newRowIds){
  int i, j; 
 newRowIds=malloc(numOfrows* sizeof(uint64_t));
  for(i=0; i<numOfrows; i++){ //gia kathe apotelesma pou epestrepse h rhj
    j=rowIds[i]; //h thesh pou epestrepse ws payload h result_list
    newRowIds[i]=interm->rowIds[updateRel][j]; //to rowId pou einai apothhkeumeno sthn thesh j tou intermediate
  }
  return newRowIds;
}

//dhmiourgei to relation gia ta orismata ths rhj, me rowIds apeutheias apo to map
relation* relFromMap(infoNode* infoMap, int rel, int col){
  int i, numOftuples;
  uint64_t* ptr;

  numOftuples= infoMap[rel].tuples; //to plhthos twn grammwn pou exei to relation

  relation* r;
  r=malloc(sizeof(relation));
  if(r==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}

  r->num_tuples=(uint64_t)numOftuples;
  r->tuples=malloc(numOftuples*sizeof(tuple));
  if(r->tuples==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}

  ptr=(uint64_t*)infoMap[rel].addr[col]; //pointer sthn arxh ths zhtoumenhs sthlhs 

  for(i=0; i<numOftuples; i++){
    r->tuples[i].payload=(uint64_t)i; //h grammh tou relation
    r->tuples[i].key=*ptr; //ta dedomena tou pointer 
    ptr++; //epomenh grammh 
  }

  if(r==NULL) return NULL;

  return r;
}

//dhmiourgia tou relation, orisma ths rhj, me rowIds apothhkeumena sto intermediate 
relation* relFromInterm(interm_node* interm, int rel, int col, int indexOfrel, infoNode* infoMap){

  int row, numOftuples,i;
  uint64_t key;

  numOftuples=interm->numOfrows[indexOfrel]; //arithmos twn apotelesmatwn sto intermediate gia thn sxesh rel pou dothhke sto query sthn thesh indexOfrel 
  relation* r=malloc(sizeof(relation));
  if(r==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}

  r->num_tuples=numOftuples;

  r->tuples=malloc(numOftuples* sizeof(tuple));
  if(r->tuples==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}

  for(i=0; i<numOftuples; i++){ 
    row=interm->rowIds[indexOfrel][i]; //to rowId pou einai apothhkeumeno sto intermediate
    key=return_value(infoMap, rel ,col, row); //ta periexomena tou relation sto rowid kai thn sthlh col apo to map
      
    r->tuples[i].payload=(uint64_t)i;//ws payload apothhkeuetai h thesh sto intermediate kai oxi to pragmatiko rowId
                                     //h epanafora ginetai me thn sunarthsh real_rowIds   

    r->tuples[i].key=(uint64_t)key;
  } 

  if(r==NULL) return NULL;

  return r; 
}


//Sth periptwsh poy exei uparksei prohgoumeno join me ta 2 relations
//ta apotelesmata vriskontai sto intermediate kai apla skanarwntai oi 2 pinakes gia ta koina values
//ginetai update to intermediate me ta nea matches kai epistrefetai
interm_node* special_sjoin(interm_node* interm, infoNode* infoMap, int  rel1,int  indexOfrel1,int rel2,int indexOfrel2, int col1, int col2, int numOfrels){
  int numOfrows,i=0,j=0;
  uint64_t ** rowIds, tuple1, tuple2, key1,key2;

  rowIds=malloc(2*sizeof(uint64_t*)); //disdiastatos pinakas pou apothhkeuei ta apotelesmata sthn 0 sthlh gia to rel1 kai sthn 1 gia to rel2 
  if(rowIds==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}

  numOfrows=interm->numOfrows[indexOfrel1];
  rowIds[0]=malloc(numOfrows*sizeof(uint64_t));
  if(rowIds[0]==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}
  rowIds[1]=malloc(numOfrows*sizeof(uint64_t));
  if(rowIds[1]==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}


  //printf("Rel1 Interm, Rel2 Interm\n");
  
  for(i=0; i<numOfrows; i++){ //gia akthe apotelesma sto intermediate
    tuple1=interm->rowIds[indexOfrel1][i];
    tuple2=interm->rowIds[indexOfrel2][i];
    key1=return_value(infoMap, rel1 ,col1, tuple1);
    key2=return_value(infoMap, rel2, col2, tuple2);

    if(key1==key2){  //an uparxei match ta rowIds apothhkeuontai
      rowIds[0][j]=tuple1;
      rowIds[1][j]=tuple2;
      j++; //plhthos apotelesmatwn
    }    
  }

  //printf("special_sjoin: total matches %d\n",j);

  interm=update_interm( interm, rowIds[0], indexOfrel1,  j, numOfrels);
  interm= update_interm( interm, rowIds[1], indexOfrel2, j,  numOfrels);
  //statusOfinterm(interm);
  
  free_rowIds(rowIds);
  return interm;
}

//sunarthsh pou kalei kai ektelei thn RadixHashJoin kai epistrefei ta apotelesmata
//epistrefei enan disdiastato pinaka me ta rows pou exei epistrepsei h result list, gia thn peraitero enhmerwsh twn relations tou intermediate 
//kai enhmerwnei 2 uint64_t pinakes enan gia akthe relation me ta rows pou prepei na ginei update to intermediate 
uint64_t **  exec_join(interm_node* interm, infoNode* infoMap, int  rel1,int  indexOfrel1,int rel2,int indexOfrel2, int col1, int col2, int numOfrels, uint64_t** rowIds1, uint64_t** rowIds2, int* numOfres, int* free_flag){
  int numOfrows;
  uint64_t ** rowIds, *newRowIds1=NULL, *newRowIds2=NULL;
  relation *relation1=NULL, *relation2=NULL;
  result* result_list=NULL;
  if(interm==NULL){ //an den uparxei intermediate
    //printf("CREATE INTERM: Rel1 Map, Rel2 Map\n");
    relation1=relFromMap(infoMap, rel1, col1); //ta 2 relations dhmiourgountai apo to map 
    relation2=relFromMap(infoMap, rel2, col2);
    
    result_list=RadixHashJoin(relation1, relation2); 

    rowIds=resToRowIds( result_list, &numOfrows); //metatroph ths result list se se uint64_t** me ta rowIds gia kathe relation
    *numOfres=numOfrows;
    
    *rowIds1=rowIds[0]; //ta rows gia to intermediate einai ayta pou epestrepse h result list
    *rowIds2= rowIds[1];
    *free_flag=-1; //den tha xreiastei free, tha ginoun apo to free tou 2d rowIds
   
     
   }
   else{ //an uparxei intermediate
   
    if(interm->numOfrows[indexOfrel1]!=-1 && interm->numOfrows[indexOfrel2]==-1){ //mono to relation 1 exei apotelesmata sto intermediate 
     // printf("Rel1 Interm, Rel2 Map\n");
  
      relation1=relFromInterm( interm, rel1, col1, indexOfrel1, infoMap); //dhmiourgia relation apo intermediate
      relation2=relFromMap(infoMap, rel2, col2); //dhmiourgia relation apo map

      result_list=RadixHashJoin(relation1, relation2);
      rowIds=resToRowIds( result_list, &numOfrows);
      *numOfres=numOfrows;
      //printf("RETURN FROM RHJ: total matches %d\n",numOfrows);

      newRowIds1 =real_RowIds( interm, rowIds[0], numOfrows, indexOfrel1, newRowIds1); //epanafora twn rowIds tou rel1 gia thn enhmerwsh tou intermediate
     
      *rowIds1=newRowIds1; //ta rows gia to intermediate tou rel1 einai ta kainourgia pou epestrepse h real_RowIds
      *rowIds2=rowIds[1]; //enw tou rel2 einai ths result_list
      *free_flag=0; //tha xreiastei free to rowIds1
   

    }
    else if(interm->numOfrows[indexOfrel1]==-1 && interm->numOfrows[indexOfrel2]!=-1){//mono to relation2 exei apotelesmata sto intermediate
      //printf("Rel1 Map, Rel2 Interm\n");
     
      relation1=relFromMap(infoMap, rel1, col1);
      relation2=relFromInterm( interm, rel2, col2, indexOfrel2, infoMap);

      result_list=RadixHashJoin(relation1, relation2);
      rowIds=resToRowIds( result_list, &numOfrows);
      *numOfres=numOfrows;
      //printf("RETURN FROM RHJ: total matches %d\n",numOfrows);

      newRowIds2 =real_RowIds( interm, rowIds[1], numOfrows, indexOfrel2, newRowIds2);
      

      *rowIds2=newRowIds2; //apotelesmata apo real_Rowids
      *rowIds1=rowIds[0]; //apo result list
      *free_flag=1; //tha xreiastei free to rowIds2

	 

    }
    else if(interm->numOfrows[indexOfrel1]!=-1 && interm->numOfrows[indexOfrel2]!=-1){ //kai ta 2 relations vriskontai sto intermediate
      //printf("Rel1 Interm, Rel2 Interm\n");
      relation1=relFromInterm( interm, rel1, col1, indexOfrel1, infoMap);
      relation2=relFromInterm( interm, rel2, col2, indexOfrel2, infoMap);

      result_list=RadixHashJoin(relation1, relation2);
      rowIds=resToRowIds( result_list, &numOfrows);
      *numOfres=numOfrows;
      //printf("RETURN FROM RHJ: total matches %d\n",numOfrows);
   

      newRowIds1 =real_RowIds( interm, rowIds[0], numOfrows, indexOfrel1, newRowIds1);
      newRowIds2 =real_RowIds( interm, rowIds[1], numOfrows, indexOfrel2, newRowIds2);
      
      
      *rowIds1=newRowIds1; //kai ta duo pairnoun ta apotelesmata tou real_RowIds gia na enhmerwsoun to intermediate
      *rowIds2=newRowIds2; 
      *free_flag=2; //tha xreiastoun kai ta 2 free

   

    }
    else if(interm->numOfrows[indexOfrel1]==-1 && interm->numOfrows[indexOfrel2]==-1){ //uparxei to intermediate alla kanena relation apo auta 
       //printf("Rel1 Map, Rel2 Map\n");
      relation1=relFromMap(infoMap, rel1, col1);
      relation2=relFromMap(infoMap, rel2, col2);
      

      result_list=RadixHashJoin(relation1, relation2);
      //print_results(result_list, &resfortest);
      rowIds=resToRowIds( result_list, &numOfrows);
      *numOfres=numOfrows;
      //printf("total matches %d\n",numOfrows );

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


//diagrafh komvou apo thn lista joinHistory (krataei poies sxeseis exoun summetasxei se koina joins)
//epstrefei thn ananevmenh lista
joinHistory* delete_nodeHistory(int indexOfrel, joinHistory** joinHist){
  int i, numOfrels;
  joinHistory* hist=*joinHist, *prev=*joinHist, *temp;
  if(hist==NULL){fprintf(stderr, "Join Hist does not exits\n"); return NULL;}
  numOfrels=hist->numOfrels;
  //anazhthsh tou relation sthn lista
  if(hist->rels[indexOfrel]!=NULL){ //an vrisketai ston prwto komvo ths listas to relation pros diagrafh
    *joinHist=hist->next; //to head ginetai o epomenos komvos
    temp=hist;
    
    for(i=0; i<numOfrels; i++){ //kanei free oti eixe ginei malloc gia ton komvo
    	if(temp->rels[i]!=NULL) free(temp->rels[i]);
    }
    free(temp); //free ton komvo
    
    return *joinHist; //epistrefetai h nea kefalh ths listas
  }
  //an den vrisketai sto head to relation
  //afairw ton komvo pou vrisketai to rel apo thn lista kai epistrefetai h kefalh 
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

//synenwsh duo komvwn joinHistory, logw join se sxeseis pou exoun ektelesei aneksarthta joins metaksu tous
//enhmerwsh ths joinHistory listas me ton kainourgio komvo kai epistrofh ths kefalhs
joinHistory* merge_nodeHistory(int indexOfrel1, int indexOfrel2, joinHistory* new, joinHistory** joinHist){
  joinHistory* temp;

  *joinHist= delete_nodeHistory( indexOfrel1, joinHist); //eueresh kai diagrafh tou komvou me to relation1 
  *joinHist=delete_nodeHistory(indexOfrel2, joinHist); //euresh kai diagarfh tou komvou me to relation2

  //anatithetai sto head ths listas o kainourgios komvos kai epistrefetai
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

//dhmiourgia kai prosthhkh komvou sthn joinHistory kai epistrofh tou teleutaiou komvou sth lista
joinHistory* add_nodeHistory(int indexOfrel,int joinedRel, joinHistory* joinHist, int numOfrels){
	joinHistory* curr=joinHist;
  int i; 
 
	if(indexOfrel>=numOfrels){fprintf(stderr, "Wrong indexOfrel <add_nodeHistory> \n" ); return NULL;}
	//an den exei dhmiourghthei h lista/ prwtos komvos
  if(joinHist==NULL){
		joinHist=malloc(sizeof(joinHistory));
		if(joinHist==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}
		joinHist->numOfrels=numOfrels; //arithmos relations pou summetexoun sto query

		joinHist->rels=malloc(numOfrels* sizeof(int*)); //disdiastatos int pinakas me ta relation pou summetexoun sto query 
                                                    //opou gia kathe relation apothhkeuei tis sxeseis pou exoun kanei mazi tou join
		if(joinHist->rels==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}
		for(i=0;i<numOfrels;i++)joinHist->rels[i]=NULL; //arxikopoihsh wste oi sxeseis pou den exoun summetasxei se join tou komvou na einai NULL

		joinHist->rels[indexOfrel]=malloc(numOfrels* sizeof(int)); //gia to relation pou klhhthkhe h add ginetai malloc
		if(joinHist->rels[indexOfrel]==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}
		for(i=1;i<numOfrels;i++)joinHist->rels[indexOfrel][i]=-1; //kai gia to relation auto epeidh einai h prwth prosthhkh join 
                                                              //oi theseis-sxeseis plin ths prwths arxikopoiountai me -1: dhladh den exoun summetasxei se join me auto to relation

		joinHist->rels[indexOfrel][0]=joinedRel; //sthn prwth thesh apothhkeuetai to relation pou egine join
		joinHist->next=NULL; //epomenos komvos NULL
    return joinHist; //epistrofh head

	}
	else{ //an uparxei hdh komvos joinHist : prosthhkh kapoiou aneksarthtou join
	 
    while(curr->next!=NULL){ //current komvos o teleutaios ths listas
      curr=curr->next;
    }
		curr->next=malloc(sizeof(joinHistory)); //ginetai malloc kai apothhkeuontai ta relations antistoixa
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
    return curr->next; //epistrefetai o teleutaios komvos ths listas

	}
	
	
}

//free thn lista joinHistory
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

//enhmerwsh enos relation tou komvou joiHistory (indexOfrel) me to relation pou eketelesane join (joinedRel)
joinHistory* update_nodeHistory(int indexOfrel, int joinedRel, joinHistory* joinHist){
    int i=0, numOfrels;
    if(joinHist!=NULL){ //kaleitai afou exei dhmiourghthei to joinHist 
      numOfrels=joinHist->numOfrels;

      if(joinHist->rels[indexOfrel]==NULL){ //an den exei summetasxei se join ksana to indexOfrel ginetai malloc o pinakas kai apothhkeuetai to joinedRel sthn prwth thesh
      	joinHist->rels[indexOfrel]=malloc(numOfrels* sizeof(int));
      	if(joinHist->rels[indexOfrel]==NULL){fprintf(stderr, "Malloc failed \n"); return NULL;}
      	for(i=1;i<numOfrels;i++)joinHist->rels[indexOfrel][i]=-1;
      	joinHist->rels[indexOfrel][0]=joinedRel;
      	return joinHist; //epistrefetai o ananewmenos komvos ths listas
      }
      else{ //diaforetika psaxnei thn prwth emfanish tou -1 kai to joinedRel apothhkeuetai ekei
      	while( i<numOfrels ){
	      if(joinHist->rels[indexOfrel][i]==-1) break;
	      i++;
      	}
      	if(i>=numOfrels){fprintf(stderr, "Updated joinHistory\n"); return NULL;}  
      	joinHist->rels[indexOfrel][i]=joinedRel;
      	return joinHist; //epistrefetai o ananewmenos komvos ths listas
      }
      
    }
    else{
      fprintf(stderr, "joinHist does not exist\n" );
      return NULL;
    }

}

//sunarthsh ektupwshs joinHIstory , den kaleitai pouthena
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


//eidikh sunarthsh gia thn anazhthsh prohgoumenou join me tis 2 sxeseis
//dinetai o komvos joinHistory pou exei vrethei to indexOfrel1 
// kai ston pinaka tou me tis sxeseis pou exoun kanei join mazi anazhtame an uparxei to indexOfrel2
//an vrethei epistrefetai 0 diaforetika 1
int find_join(joinHistory* joinHist, int indexOfrel1, int indexOfrel2){
	int i, numOfrels;
	if(joinHist==NULL)return 1; //kanena relation den uparxei sto joinHist
	numOfrels=joinHist->numOfrels;
	if(joinHist->rels[indexOfrel1]==NULL){fprintf(stderr, "find_join indexOfrel1 does not exist\n" ); exit(0);} //to indexOfrel1 den exei kanei join me kamia sxesh, einai kai error
	for(i=0; i<numOfrels; i++){
		if(joinHist->rels[indexOfrel1][i]==indexOfrel2) return 0; //to indexOfrelation1 exei kanei join me to 2
	}
	return 1;//den vrethhke
}

//h sunarthsh pou ektelei join, enhmerwnei to intermediate kai thn joinHistory
//epistrefei ton intermediate komvo meta to join
interm_node* join2(interm_node* interm, infoNode* infoMap, joinHistory** joinHist, int  rel1,int  indexOfrel1,int rel2,int indexOfrel2, int col1, int col2, int numOfrels){
   
  int hold1=-1, hold2=-1, to_add,  numOfrows=0, i, j ,free_flag;
  joinHistory* currHist1, *currHist2, *new;
  joinHistory* curr;
  uint64_t* temp, *rowIds1, *rowIds2, **updateIds;
  currHist1=*joinHist;
  currHist2=*joinHist;

  //anazhthsh sthn lista joinHistory an kapoio relation apo ta 2 exei summetasxei se prohgoumeno join
  while(currHist1!=NULL){
    if(currHist1->rels[indexOfrel1]!=NULL) {hold1=indexOfrel1; break;} //an vrethei komvos pou to relation1 exei ektelesei prohgoumeno join apothhkeuetai sto currHist1
    currHist1=currHist1->next;
  }
  while(currHist2!=NULL){
    if(currHist2->rels[indexOfrel2]!=NULL) { hold2=indexOfrel2; break;}//an vrethei komvos pou to relation2 exei ektelesei prohgoumeno join apothhkeuetai sto currHist2
    currHist2=currHist2->next;
  }

   //statusOfinterm(interm);
 
  //an kai ta 2 relations vriskontai ston idio komvo joinHistory 
  if((currHist1==currHist2)&&(currHist2!=NULL)&&(currHist1!=NULL)) {
    
    //anazhthsh an exoun summetasxei mazi se prohoumeno join wste na sarwthei to intermediate gia ta apotelesmata
  	if(find_join(currHist1, indexOfrel1, indexOfrel2)==0){
  		//printf("eidiko self join\n");
  		interm=special_sjoin( interm, infoMap, rel1, indexOfrel1, rel2, indexOfrel2, col1, col2, numOfrels); //sarwsh tou intermediate kai apothhkeush twn kainourgiwn matches
  	}
  	else{ //an den exoun ektelesei mazi kapoio join

  		//klhsh ths RadixHashJoin
      updateIds=exec_join( interm, infoMap,  rel1, indexOfrel1, rel2, indexOfrel2, col1, col2,  numOfrels, &rowIds1, &rowIds2, &numOfrows, &free_flag);
  		//ta updateIds einai enas disdiastatos pinakas uint64 me updateIds[0] gia to rel1 tou join kai updateIds[1] gia to rel2
      //Xrhsimopoiountai gia thn enhmerwsh tou intermediate twn relations pou exoun kanei ksana join me tis sxeseis rel1 kai rel2
      //An h RadixHashJoin exei parei san orisma relation pou ta keys exoun proelthei apo rowIds tou intermediate
      //epistrefei ta rows tou intermediate kai oxi tou map, etsi oi upoloipoes sxeseis pou exoun kanei join mazi tous
      //enhmerwnontai me auta ta rows tou intermediate, pou omws ta RowIds antistoixoun se autes  

      //enhmerwsh tou intermediate me ta rowIds pou epestrepse h exec_join sta rowIds1 kai rowIds2
      interm= update_interm(interm, rowIds1, indexOfrel1, numOfrows,  numOfrels);
  		interm=update_interm( interm, rowIds2, indexOfrel2,  numOfrows, numOfrels);
       
	  	
      //Enhmerwnetai to intermediate gia tis upoloipes sxeseis tou komvou JoinHistory pou vrethhkan ta relations tou join.
	  	for(i=0; i<numOfrels; i++) {
	       if( (currHist1->rels[i])!=NULL && i!=indexOfrel1 && i!=indexOfrel2){ //an einai apothhkeumenh sxesh sto komvo joinHist 
                                                                              //kai oxi h 2 pou ektelesan to join (exei hdh enhmerwthei to intermediate me autes)
	        
          temp= real_RowIds( interm, updateIds[0], numOfrows, i, temp); //enhmerwnetai to temp me ta nea rowIds gia thn sxesh, me vash ta lines pou epestrepse h result_list
                                                                        //efoson vriskontan ston idio komvo joinHist ta rows tou intermediate pou tha epistrepsei h result_list
                                                                        //einai idia, dinetai to updateIds[0] 
	        
	        interm= update_interm(interm, temp, i, numOfrows,  numOfrels); //enhmerwnetai to intermediate me to temp pou epestrepse h real_RowIds
	         //statusOfinterm(interm);

	        free(temp);
	      }      
	    }
	   currHist1=update_nodeHistory(indexOfrel2,indexOfrel1, currHist1); //prostithetai sthn istoria twn Joins ta duo relations
     currHist1=update_nodeHistory(indexOfrel1,indexOfrel2, currHist1);
      print_joinHist(*joinHist);
      

     if(free_flag>-1){
      if(free_flag==0){free(rowIds1);}
      else if(free_flag==1){free(rowIds2);}
      else{free(rowIds1); free(rowIds2);}
     }
      free_rowIds(updateIds);

  	}
   
    return interm;
    
  }

  //efoson den vriskontai ston idio joinHist komvo
  //ekteleitai h Radix Hash Join kai enhmerwnetai to intermediate me ta apotelesmata
  updateIds=exec_join( interm, infoMap,  rel1, indexOfrel1, rel2, indexOfrel2, col1, col2,  numOfrels, &rowIds1, &rowIds2, &numOfrows,&free_flag);
  interm= update_interm(interm, rowIds1, indexOfrel1, numOfrows,  numOfrels);
  interm=update_interm( interm, rowIds2, indexOfrel2,  numOfrows, numOfrels);

  //an den exei prohghthei join me kamia apo tis sxeseis rel1 rel2
  if(currHist1==NULL && currHist2==NULL && hold1==-1 && hold2==-1 ){
    if(*joinHist==NULL){ //an einai to prwto join pou ekteleitai
       *joinHist=add_nodeHistory(indexOfrel1,indexOfrel2,*joinHist,numOfrels);//dhmiourgeitai o prwtos komvos joinHistory
      *joinHist=update_nodeHistory(indexOfrel2,indexOfrel1, *joinHist); //ginetai update to rel2 sto komvo auto kai epistrefei to head enhmerwmeno
      
      //!!!!ligo sad, to xw grapsei xalia Lor isws na to allaksoume ,h update gurnaei ton komvo pou phre opote den enhmerwnei to head alliws an dineis to curr
    }
    else{ //an uparxei hdh psaxnei se poion komvo prepei na enhmerwthei
      curr=add_nodeHistory(indexOfrel1,indexOfrel2, *joinHist, numOfrels); //enhmerwnei kai epistrefei ton kainourgio komvo 
      curr=update_nodeHistory(indexOfrel2,indexOfrel1, curr); //kane update ton komvo auto gia to rel2
       
    
    }   
     print_joinHist(*joinHist);

    if(free_flag>-1){
      if(free_flag==0){free(rowIds1);}
      else if(free_flag==1){free(rowIds2);}
      else{free(rowIds1); free(rowIds2);}
     } 
    free_rowIds(updateIds);
    return interm;
  }

  //to rel1 uparxei se komvo tou joinHistory (exei summetasxei se prohgoumeno join) enw to rel2 oxi 
  if(currHist1!=NULL && currHist2==NULL) {
    //enhmerwse to intermediate gia tis sxeseis pou exoun kanei prohgoumena joins me to rel1
    for(i=0; i<numOfrels; i++) {
       if( (currHist1->rels[i])!=NULL && i!=indexOfrel1){
        
        temp= real_RowIds( interm, updateIds[0], numOfrows, i, temp);
        
        interm= update_interm(interm, temp, i, numOfrows,  numOfrels);
        free(temp);
      }
      
    }
    currHist1=update_nodeHistory(indexOfrel2,indexOfrel1, currHist1); //enhmerwse ton
     currHist1=update_nodeHistory(indexOfrel1,indexOfrel2, currHist1);
     print_joinHist(*joinHist);
   
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
    currHist2=update_nodeHistory(indexOfrel1,indexOfrel2, currHist2);
    currHist2=update_nodeHistory(indexOfrel2,indexOfrel1, currHist2);
    print_joinHist(*joinHist);
  
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
   print_joinHist(*joinHist);
   
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
    //statusOfCross(interm,toMul,numOfrels);  
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
 // print_joinHist(*joinHist);

  //ektypwnw thn lista twn previous na dw an tairiazei me ta joins
 /* if(j!=0){
    printf("--crossed: ");
    for(i=0;i<j;i++){
      if(i==j-1) printf("%d ",prevs[i]);
      else printf("%d, ",prevs[i]);
    }
    printf("\n\n");
  }
*/
 // statusOfCross(interm,toMul,numOfrels);  

  free(prevs);
  return toMul;
}

void statusOfCross(interm_node* interm, long long int* toMul, int numOfrels){
  int i;

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
