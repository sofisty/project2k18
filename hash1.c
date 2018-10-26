#include "hash1.h"


int hash_func(int value, int n){ //hash function gia thn tmhmatopoish , dimiourgia evretiriwn sta R kai S
	int l=1;
	for(int i =0; i<n; i++) l*=2;
	return (value%l);
}

hist_node*  update_hist(hist_node* head, int hash_val, int* total_buckets){ // dimiourgei h enhmerwnei to dothen istogramma
	hist_node* temp;
	int flag=0;
	if(head==NULL){ //dimiourgei to keno dothen istogramma
		head = malloc(sizeof(hist_node));
		if (head == NULL) {
	    fprintf(stderr, "Malloc failed \n"); return NULL;
		}

		head->hash_val = hash_val; //arxikopoiw to istogramma me thn pdothisa hashvalue
		head->count=1;
		head->next=NULL;
		*total_buckets=1;
		return head;
	}	
	hist_node * current = head;
	if(current->hash_val==hash_val){ current->count+=1; return head; }//an yparxei h dotheisa hash value sto istogramma auksanw to count
	if(current->hash_val>hash_val){// alliws einai megaliteri i timh prosthetw tin nea hash value sto istogramma 
		temp=head;
		head = malloc(sizeof(hist_node));
		if (head == NULL) {
	    fprintf(stderr, "Malloc failed \n"); return NULL;
		}

		head->hash_val = hash_val;
		head->count=1;
		head->next=temp;
		*total_buckets+=1;
		return head;
	}

    while (current->next != NULL) { //an h timh yparxei hdh psaxnw pou einai(plin tis prwths timhs) kai auksanw to count
    	if(current->next->hash_val==hash_val) { current->next->count+=1; return head; }
    	if(current->next->hash_val<hash_val){  //an den iparxei psaxnw pou tha to prosthesw
    		current = current->next;
       		
    	}
    	else{ flag=1; break;} //to current next exei megalutero value

        
    }
    if(flag==1){ //kathorizw to temp ws ti telutaia timh tou istogrammatos
    	temp=current->next;
    }
    else{
    	temp=NULL;
    }
    
    current->next = malloc(sizeof(hist_node)); //dimiourgw sti thesi pou vrika ti nea eggrafi tou istogrammatos
    if (current->next == NULL) {
	    fprintf(stderr, "Malloc failed \n"); return NULL;
	}
    current->next->hash_val = hash_val;
    current->next->count=1;
    current->next->next = temp;
    *total_buckets+=1;
    

    return head;//epistrefw tin kefalh tou istogrammatos 

}

void print_hist(hist_node * head) { //typwnei to istogramma sth parousa katastash sou
    hist_node * current = head;

    while (current != NULL) {
        printf("Hash value: %d includes %d items\n", current->hash_val, current->count);
        current = current->next;
    }
}

void free_hist(hist_node* head){ //kanei free thn memory pou exei desmeutei gia to istogramma
	hist_node* temp=head;
	hist_node* curr=head;

	while(curr->next!=NULL){
		temp=curr;
		curr=curr->next;
		free(temp);
		temp=NULL;
	}
	free(curr);
	curr=NULL;
	
}


psum_node* create_psumlist(psum_node* psum_head, hist_node* hist_head ){ //dhmiourgei thn lista me ta psum
	 int offset=0;
	 psum_node* curr;

	 hist_node* hist_current=hist_head;
	 while(hist_current!=NULL){ // oso iparxoun eggrafes so istogramma pou exei dothei
	 	if(psum_head==NULL){ // an to psum list einai keno to dimiourgei eksarxeis
			psum_head=malloc(sizeof(psum_node));
			if (psum_head == NULL) { fprintf(stderr, "Malloc failed \n"); return NULL;}
			psum_head->hash_val=hist_current->hash_val; //kai to arxikopoiei me tin dotheia hash value
			psum_head->offset=offset;
			psum_head->curr_off=offset; //apothikevei to offset apo to opoio ksekinaei sto relation to tmima me authn thn hash value
			psum_head->next=NULL;
			curr=psum_head;

		}
		else{
			if(curr->next==NULL){ //alliws prosthetw sto telos tou psum_list tin current eggrafi tou istogrammatos
				curr->next=malloc(sizeof(psum_node));
				if (curr == NULL) {fprintf(stderr, "Malloc failed \n"); return NULL;}
				curr->next->hash_val=hist_current->hash_val;
				curr->next->offset=offset;
				curr->next->curr_off=offset;
				curr->next->next=NULL;
				curr=curr->next;
			}
			//else lathos 
			
		}
		offset+=hist_current->count;//proxwraei to offset oses einai oi eggrafes pou exoun hash value oso to eksetazomeno (to pairnei apo to count tou hist)
	 	hist_current=hist_current->next;
	 }


   return psum_head; //epistrefei tin kefalh ths listas

}


void print_psum(psum_node * head) { //ektypwnei tin psum lista
    psum_node * current = head;

    while (current != NULL) {
        printf("Hash value: %d starts from %d \n", current->hash_val, current->offset);
        //printf("\tCurrent offset %d\n", current->curr_off);
        current = current->next;
    }
}


void free_psum(psum_node* head){ //apodesmevei thn mnh mou eixame dwsei sthn psum lista
	psum_node* temp=head;
	psum_node* curr=head;

	while(curr->next!=NULL){
		temp=curr;
		curr=curr->next;
		free(temp);
		temp=NULL;
	}
	free(curr);
	curr=NULL;

	
}


int search_Psum(psum_node* head, int key, int n ){ //psaxnei tin psum lista gia na vrei to offset sto opoio tha prostethei to key
	psum_node* curr=head;
	int hash_val;
	int offset;
	
	hash_val=hash_func(key, n);//vriskei thn hashvalue tou dothentos key

	while(curr!=NULL){ //oso exei eggrafes h psum
		if(curr->hash_val==hash_val){ //psaxnw to sygekrimeno hash value
			offset=curr->curr_off;
			curr->curr_off++;
			return offset;//epistrefei to offset
		}
		curr=curr->next;
	}
	return 1;
}


relation* reorder_R(psum_node* phead, relation* R, relation* R_new, int n  ){ //kanei reorder na relation me vasi thn psum list
	int i, size=R->num_tuples;
	int  offset, key, payload;
	R_new->num_tuples=size;
	R_new->tuples=malloc(size* sizeof(tuple));
	if(R_new->tuples==NULL){ fprintf(stderr, "Malloc failed \n"); return NULL;}

	for(i=0; i<size; i++){ //ftiaxnei to R_new pou tha epistrafei me thn seira etsi wste na armozei h tmhmatopoihsh tou
						   // me auth thn psum kai tou isogrammatos
		key=R->tuples[i].key;
		payload=R->tuples[i].payload;
		offset=search_Psum( phead, key, n );
		//print_psum( phead);
		R_new->tuples[offset].key=key;
		R_new->tuples[offset].payload=payload;
	}
	return R_new; //epistrefei th nea anadiorganwmenh sxesh

}

void print_R(relation* R){ //ektipwnei ta tuples ths relation pou dinetai
	int i, size=R->num_tuples;
	printf("Relation num_tuples: %d\n",size );
	for(i=0; i<size; i++){
		printf(" [%d]-> %d\n",i, R->tuples[i].key );
	}
}

//----------------------------------------------------------------------------------


result* store_results(result* result_list, result** curr_res, tuple resultR, tuple resultS ){ //apothikevei enan sindiasmo tuple sthn lista apotelesmatwn
	struct result* curr= *curr_res;
	int count;
	int size=(1024*1024)/16;//arithmos eggrafwn pou xwrane sto ena bucket ths listas
	if(result_list==NULL){//an exei dothei keni lista tote dimiourgw kainourgia lista apotelesmatwn
		result_list=malloc(sizeof(result));
		if (result_list == NULL) { fprintf(stderr, "Malloc failed \n"); return NULL;}
		result_list->tuplesR=malloc(size* sizeof(tuple));
		if (result_list->tuplesR == NULL) { fprintf(stderr, "Malloc failed \n"); return NULL;}

		result_list->tuplesS=malloc(size* sizeof(tuple));
		if (result_list->tuplesS == NULL) { fprintf(stderr, "Malloc failed \n"); return NULL;}
		
		result_list->next=NULL;
		result_list->tuplesR[0]=resultR; //kai arxikopoiw thn lista apotelesmatwn me ta dothenta tuples
		result_list->tuplesS[0]=resultS;
		result_list->count=1; //auksanw ton arithmo eggrafwn tis listas kata 1
		*curr_res=result_list;// krataw thn thesi pou exei ftasei h lista apotelesmatwn gia na th xrhsimopoihsw argotera
		return result_list;//epistrefw th lista

	}
	else{//ean den einai kenh, prosthetw to apotelesma (sindiasmo tuples )sth lista

		if(curr->count<size){ //an to apotelesma xwraei sto bucket pou vriskomai, to kataxwrw ekei
			count=curr->count;
			curr->tuplesR[count]=resultR;
			curr->tuplesS[count]=resultS;
			curr->count+=1;
			*curr_res=curr;//krataw thn trexousa thesh
			return result_list;//epistrefw th lista
		}
		else if(curr->count==size){// an h lista den exei xwro, desmevw xwro enos bucket akomh sth lista kai to arxikopoiw me to dothen tup
			curr->next=malloc(sizeof(result));
			if (curr->next == NULL) { fprintf(stderr, "Malloc failed \n"); return NULL;}
			curr->next->tuplesR=malloc(size* sizeof(tuple));
			if (curr->next->tuplesR == NULL) { fprintf(stderr, "Malloc failed \n"); return NULL;}
			
			curr->next->tuplesS=malloc(size* sizeof(tuple));
			if (curr->next->tuplesS == NULL) { fprintf(stderr, "Malloc failed \n"); return NULL;}
			
			
			curr->next->tuplesR[0]=resultR;
			curr->next->tuplesS[0]=resultS;

			curr->next->count=1; //auksanw kata ena to count tou neou bucket
			curr->next->next=NULL;
			*curr_res=curr->next;//krataw ti thesh pou vriskomai sth listsa
			return result_list;//epistrefw th lista

		}
		else{fprintf(stderr, "Count is wrong \n"); return NULL;}
	}
}

void print_results(result* result_list){ //ektypwnw th lista apotelesmatwn
	int count, i, b=0, total=0;
	result* curr=result_list;
	printf("--RESULTS--\n");
	if(result_list==NULL)printf("0 results found!\n");
	while(curr!=NULL){
		printf("\n-Bucket: %d\n", b);
		count=curr->count;
		for(i=0; i<count; i++){
			total++;
			printf(" Matchin Keys %d=%d Payload R %d, Payload S %d\n", curr->tuplesR[i].key, curr->tuplesS[i].key, curr->tuplesR[i].payload, curr->tuplesS[i].payload);
		}
		curr=curr->next;
		b++;
	}

	printf("~~~Total results %d ~~~~~\n",total );
}

// To index  einai ena flag pou mou leei se poio apo ta dyo buckets (pou exei epileksei h compare_buck) exei ginei to hash2
//kalw thn sunarthsh kai sto orisma tou R vazw to relation pou exei to index enw sto orisma tou S to allo bucket

result* search_results(result* result_list, relation* S_new, int startS, int endS, int** bucket, int** chain, relation* R_new, int startR, int hash_size,  int index){ //ston R exei xtistei to eurethrio 
	int i, b,c, hash_val;//  b:bucket, c:chain
	int offset=startR;
	result* result_cur= result_list;

	for(i=startS; i<endS; i++){ //gia oles tis times pou periexei to bucket sto opoio exei ginei to bucket-chain hashing
		hash_val=hash2_func(S_new->tuples[i].key,hash_size);
		
		
		if(bucket[hash_val]!=-1){ //An to key tou relation pou den exei ginei hash2 yparxei sto relation pou einai b-c hashed
			
			b=*bucket[hash_val]; //h timh auth einai h teleutaia tou b-c hashed relation pou exei authn thn hash val
		
			if(b!=-1){
				c=*chain[b];
				//printf("C: %d\n",c );
				if(R_new->tuples[b+offset].key==S_new->tuples[i].key){ //an to index den einai 1 shmainei oti sthn thesh tou r exoume dwsei to s
					
					if(index!=0){result_list= store_results(result_list,&result_cur, S_new->tuples[i],  R_new->tuples[b+offset] );} //opote gia na apothhkeusei swsta ta apotelesmata
																													//dinoume anapoda ta orismata 
					else{result_list= store_results(result_list,&result_cur, R_new->tuples[b+offset],  S_new->tuples[i] );}

					//printf("Bucket row %d 	Match 	R:%d = S:%d\n",b, bucketR[b].key, bucketS[i].key );
				}
				else{
					//printf("Bucket: Keys do not match: HASH %d and R:%d != S:%d \n",hash_val, bucketR[b].key,bucketS[i].key );
				}
				while(c!=-1){ //an de vriskomai sthn prwth-prwth kew me auth thn hash val
					if(R_new->tuples[c+offset].key==S_new->tuples[i].key){
						
						if(index!=0){result_list= store_results(result_list,&result_cur, S_new->tuples[i],  R_new->tuples[c+offset] );}
						else{result_list= store_results(result_list, &result_cur, R_new->tuples[c+offset] , S_new->tuples[i] );}
						//printf("\tChain row %d 	Match 	R:%d = S:%d\n",c,bucketR[c].key, bucketS[i].key );
					}
					else{
						//printf("\tChain Keys do not match: HASH %d and R:%d != S%d\n",hash_val, bucketR[c].key, bucketS[i].key );
					}
					c=*chain[c];
				}

				

			}

		}
		else{
				//printf("Bucket: -- S:%d does not exist in R!\n", bucketS[i].key );
		}

	}


	
	return result_list;//epistrerfw thn lista apotelesmatwn

} 

void free_result_list(result* result_list){ //apodesmevw thn mnhmh pou edwsa sth lista apotelesmatwn
	result* temp= result_list;
	result* curr=result_list;
	while(curr!=NULL){
		temp=curr;
		curr=curr->next;
		free(temp->tuplesR);
		temp->tuplesR=NULL;
		free(temp->tuplesS);
		temp->tuplesS=NULL;
		free(temp);
		temp=NULL;
	}

}
