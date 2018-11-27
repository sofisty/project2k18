#include "results.h"


#define n 8
#define size (((1024*1024)/24)-1) //arithmos eggrafwn pou xwrane sto ena bucket ths listas 

result* store_results( result** head,result* curr_res, tuple resultR, tuple resultS ){ //apothikevei enan sindiasmo tuples sthn lista apotelesmatwn

	int count,index;
	
	//printf("SIZE %d\n",size );

	
	if( curr_res==NULL){//an exei dothei keni lista tote dimiourgw kainourgia lista apotelesmatwn
		 curr_res=malloc(sizeof(result));
		if (curr_res == NULL) { fprintf(stderr, "Malloc failed \n"); return NULL;}
		curr_res->matches=malloc(size*3* sizeof(uint64_t));
		if (curr_res->matches== NULL) { fprintf(stderr, "Malloc failed \n"); return NULL;}
		
		curr_res->next=NULL;
		curr_res->matches[0]=resultR.payload; //kai arxikopoiw thn lista apotelesmatwn me ta dothenta tuples
		curr_res->matches[1]=resultS.payload;
		curr_res->matches[2]=resultR.key;
		curr_res->count=1; //auksanw ton arithmo eggrafwn tis listas kata 1
		// krataw thn thesi pou exei ftasei h lista apotelesmatwn gia na th xrhsimopoihsw argotera
		*head=curr_res;
		return curr_res;//epistrefw th lista

	}
	else{//ean den einai kenh, prosthetw to apotelesma (sindiasmo tuples )sth lista

		if(curr_res->count<size){ //an to apotelesma xwraei sto bucket pou vriskomai, to kataxwrw ekei
				//printf("woo hoo\n");
			
			count=curr_res->count;
			index=count*3;
			curr_res->matches[index]=resultR.payload; //kai arxikopoiw thn lista apotelesmatwn me ta dothenta tuples
			curr_res->matches[index+1]=resultS.payload;
			curr_res->matches[index+2]=resultR.key;
			curr_res->count+=1;
			//epistrefw thn trexousa thesh
			return curr_res;

		}
		else if(curr_res->count==size){// an h lista den exei xwro, desmevw xwro enos bucket akomh sth lista kai to arxikopoiw me to dothen tup
			 
			curr_res->next=malloc(sizeof(result));
			if (curr_res->next == NULL) { fprintf(stderr, "Malloc failed \n"); return NULL;}
			curr_res->next->matches=malloc(size*3* sizeof(uint64_t));
			if (curr_res->next->matches== NULL) { fprintf(stderr, "Malloc failed \n"); return NULL;}
			
			
			curr_res->next->matches[0]=resultR.payload; //kai arxikopoiw thn lista apotelesmatwn me ta dothenta tuples
			curr_res->next->matches[1]=resultS.payload;
			curr_res->next->matches[2]=resultR.key;

			curr_res->next->count=1; //auksanw kata ena to count tou neou bucket
			curr_res->next->next=NULL;
			//epistrefw ti thesh pou vriskomai sth listsa
			return curr_res->next;//epistrefw th lista

		}
		else{fprintf(stderr, "Count is wrong \n"); return NULL;}
	}
}

void print_results(result* result_list, int* resfortest){ //ektypwnw th lista apotelesmatwn
	int count, i, b=0, total=0,index;
	result* curr=result_list;
	printf("--RESULTS--\n");
	if(result_list==NULL)printf("0 results found!\n");
	while(curr!=NULL){
		//printf("\n-Bucket: %d\n", b);

		count=curr->count;
		for(i=0; i<count; i++){
			total++;
			index=3*i;
			//printf(" Matchin Keys %ld=%ld Payload R %ld, Payload S %ld\n", curr->matches[index+2], curr->matches[index+2], curr->matches[index],curr->matches[index+1]);
		}
		curr=curr->next;
		b++;
	}
	*resfortest=total;
	printf("~~~Total results %d ~~~~~\n",total );
}


// To index  einai ena flag pou deixnei se poio apo ta dyo buckets (pou exei epileksei h compare_buck) exei ginei to hash2
//kalw thn sunarthsh kai sto orisma tou R vazw to relation pou exei to index enw sto orisma tou S to allo bucket

result* search_results(result** head,result* curr_res, relation* S_new, int startS, int endS, int** bucket, int** chain, relation* R_new, int startR, int hash_size,  int index){ //ston R exei xtistei to eurethrio 
	int i, b,c, hash_val;//  b:bucket, c:chain
	int offset=startR;
	

	for(i=startS; i<endS; i++){ //gia oles tis times pou periexei to bucket sto opoio den exei ginei to index bucket-chain
		hash_val=hash2_func(S_new->tuples[i].key,hash_size);
		
		
		if(*bucket[hash_val]!=-1){ //An to key tou relation pou den exei ginei hash2 yparxei sto relation pou einai b-c hashed
			
			b=*bucket[hash_val]; //o prwtos deikths gia thn epomenh eggrafh apo ton bucket ston chain pinaka
		
			if(b!=-1){ 
				c=*chain[b]; //o epomenos deikths tou chain
				//printf("C: %d\n",c );
				if(R_new->tuples[b+offset].key==S_new->tuples[i].key){ //an uparxei match timwn
					
					if(index!=0){curr_res= store_results(head,curr_res, S_new->tuples[i],  R_new->tuples[b+offset] );} //apothhkeuei ta tuples
																													
					else{curr_res= store_results(head,curr_res, R_new->tuples[b+offset],  S_new->tuples[i] );}//an to index den einai 1 shmainei oti sthn thesh tou r exoume dwsei to s
																											//opote gia na apothhkeusei swsta ta apotelesmata
																											//dinoume anapoda ta orismata 
					//printf("Bucket row %d 	Match 	R:%d = S:%d\n",b, bucketR[b].key, bucketS[i].key );
				}
				else{
					//printf("Bucket: Keys do not match: HASH %d and R:%d != S:%d \n",hash_val, bucketR[b].key,bucketS[i].key );
				}
				while(c!=-1){ //oso uparxoun eggrafes apo thn alysida tou chain kai o deikths den einai-1
					if(R_new->tuples[c+offset].key==S_new->tuples[i].key){
						if(index!=0){curr_res= store_results(head,curr_res, S_new->tuples[i],  R_new->tuples[c+offset] );}
						else{curr_res= store_results(head,curr_res, R_new->tuples[c+offset] , S_new->tuples[i] );}
						//printf("\tChain row %d 	Match 	R:%d = S:%d\n",c,bucketR[c].key, bucketS[i].key );
					}
					else{
						//printf("\tChain Keys do not match: HASH %d and R:%d != S%d\n",hash_val, bucketR[c].key, bucketS[i].key );
					}
					c=*chain[c]; //enhmerwsh deikth me to periexomeno tou chain
				}

				

			}

		}
		else{
				//printf("Bucket: -- S:%d does not exist in R!\n", bucketS[i].key );
		}

	}


	
	return curr_res;//epistrerfw thn lista apotelesmatwn

} 

uint64_t** resToRowIds(result* result_list, int* numOfr){
	int  index,i;
	int numOfrows=0;
	//desmevw mnimi gia ton diasdiastato 
	uint64_t** rowIds=malloc(2*sizeof(uint64_t*));
	if (rowIds== NULL) { 
		fprintf(stderr, "Malloc failed \n"); 
		exit(-1);
	}
	result* curr=result_list;
	while(curr!=NULL){
		numOfrows+=curr->count;
		curr=curr->next;
		
	}	

	//desmevw mnhmh ggia tous monous pinakes row ids
	rowIds[0]=malloc(numOfrows*sizeof(uint64_t));
	if (rowIds[0]== NULL) { 
		fprintf(stderr, "Malloc failed \n"); 
		exit(-1);
	}
	rowIds[1]=malloc(numOfrows*sizeof(uint64_t));
	if (rowIds[1]== NULL) { 
		fprintf(stderr, "Malloc failed \n"); 
		exit(-1);
	}

	curr=result_list;
	int j=0;
	while(curr!=NULL){

		for(i=0; i<curr->count; i++){
			index=i*3;

			rowIds[0][j]=result_list->matches[index];
			rowIds[1][j]=result_list->matches[index+1];

			j++;
		}
		
		curr=curr->next;

	}	
	*numOfr=numOfrows;
	return rowIds;
}

void free_result_list(result* result_list){ //apodesmevw thn mnhmh pou edwsa sth lista apotelesmatwn
	result* temp= result_list;
	result* curr=result_list;
	while(curr!=NULL){
		temp=curr;
		curr=curr->next;
		free(temp->matches);
		temp->matches=NULL;
		free(temp);
		temp=NULL;
	}

}



result* RadixHashJoin(relation *R, relation *S){
	int i;
	result* result_list=NULL;

	relation* R_new=malloc(sizeof(relation));
	if (R_new == NULL) { fprintf(stderr, "Malloc failed \n"); return NULL;}

	hist_node * R_head;
	R_head=malloc(256* sizeof(hist_node));
	if (R_head == NULL) { fprintf(stderr, "Malloc failed \n"); return NULL;}
	

	psum_node* phead;
	phead=malloc(256* sizeof(psum_node));
	if (phead == NULL) { fprintf(stderr, "Malloc failed \n"); return NULL;}

	for(i=0; i<256; i++){ R_head[i].count=0; }
	R_head=update_hist(R_head, R, n);
	
	phead=update_psumlist(phead,R_head);	
	
 	R_new=reorder_R(phead,  R,  R_new,  n  );//anadiorganwnw to relation R


 	//print_R( R);
 	
 	relation* S_new=malloc(sizeof(relation));
	if (S_new == NULL) { fprintf(stderr, "Malloc failed \n"); return NULL;}

	hist_node* S_head;
	S_head=malloc(256* sizeof(hist_node));
	if (S_head == NULL) { fprintf(stderr, "Malloc failed \n"); return NULL;}
	

	psum_node* S_phead;
	S_phead=malloc(256* sizeof(psum_node));
	if (S_phead == NULL) { fprintf(stderr, "Malloc failed \n"); return NULL;}


	for(i=0; i<256; i++){ S_head[i].count=0; }
	S_head=update_hist(S_head, S, n);
	
	S_phead=update_psumlist(S_phead,S_head);	
	
 	S_new=reorder_R(S_phead,  S,  S_new,  n  );//anadiorganwnw to relation R

 	
 	result_list= final_hash(R_head, S_head, phead, S_phead, R_new, S_new); //kalw thn synarthsh pou kanei to b-c hashing kai ola ta results

 	//free(R_head);
 
	//free(S_head);

 	//free(phead);
 
 	//free(S_phead);
 	

 	//free(R_new->tuples);
    
    free(S_new->tuples);
	free(R_new);
	free(S_new);

 	return result_list;


}
