#include "hash1.h"


int hash_func(int value, int n){
	int l=1;
	for(int i =0; i<n; i++) l*=2;
	//printf("l is %d\n",l ); 
	return (value%l);
}


hist_node*  update_hist(hist_node* head, int hash_val, int* total_buckets){
	if(head==NULL){
		head = malloc(sizeof(hist_node));
		if (head == NULL) {
	    fprintf(stderr, "Malloc failed \n"); return NULL;
		}

		head->hash_val = hash_val;
		head->count=1;
		*total_buckets=1;
		return head;
	}	
	hist_node * current = head;
	if(current->hash_val==hash_val){ current->count+=1; return head; }

    while (current->next != NULL) {

        current = current->next;
        if(current->hash_val==hash_val) { current->count+=1; return head; }
    }

    current->next = malloc(sizeof(hist_node));
    if (current->next == NULL) {
	    fprintf(stderr, "Malloc failed \n"); return NULL;
	}
    current->next->hash_val = hash_val;
    current->next->count=1;
    current->next->next = NULL;
    *total_buckets+=1;
    

    return head;

}

void print_hist(hist_node * head) {
    hist_node * current = head;

    while (current != NULL) {
        printf("Hash value: %d includes %d items\n", current->hash_val, current->count);
        current = current->next;
    }
}



psum_node* create_psumlist(psum_node* psum_head, hist_node* hist_head ){
	 int offset=0;
	 psum_node* curr;

	 hist_node* hist_current=hist_head;
	 while(hist_current!=NULL){
	 	if(psum_head==NULL){
			psum_head=malloc(sizeof(psum_node));
			if (psum_head == NULL) { fprintf(stderr, "Malloc failed \n"); return NULL;}
			psum_head->hash_val=hist_current->hash_val;
			psum_head->offset=offset;
			psum_head->curr_off=offset;
			psum_head->next=NULL;
			curr=psum_head;

		}
		else{
			if(curr->next==NULL){
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
		offset+=hist_current->count;
	 	hist_current=hist_current->next;
	 }


   return psum_head;

}


void print_psum(psum_node * head) {
    psum_node * current = head;

    while (current != NULL) {
        printf("Hash value: %d starts from %d \n", current->hash_val, current->offset);
        //printf("\tCurrent offset %d\n", current->curr_off);
        current = current->next;
    }
}


int search_Psum(psum_node* head, int key, int n ){
	psum_node* curr=head;
	int hash_val;
	int offset;
	
	hash_val=hash_func(key, n);

	while(curr!=NULL){
		if(curr->hash_val==hash_val){
			offset=curr->curr_off;
			curr->curr_off++;
			printf("Epistrefw offset: %d\n",offset );
			return offset;
		}
		curr=curr->next;
	}
	//printf("WROOOOOONG\n");
	return 1;
}


relation* reorder_R(psum_node* phead, relation* R, relation* R_new, int n  ){
	int i, size=R->num_tuples;
	int  offset, key, payload;
	//printf(" SIZE %d\n",size );
	R_new->num_tuples=size;
	R_new->tuples=malloc(size* sizeof(tuple));
	if(R_new->tuples==NULL){ fprintf(stderr, "Malloc failed \n"); return NULL;}

	for(i=0; i<size; i++){

		key=R->tuples[i].key;
		payload=R->tuples[i].payload;
		offset=search_Psum( phead, key, n );
		//print_psum( phead);
		R_new->tuples[offset].key=key;
		R_new->tuples[offset].payload=payload;



	}
	return R_new;

}

void print_R(relation* R){
	int i, size=R->num_tuples;
	printf("Relation num_tuples: %d\n",size );
	for(i=0; i<size; i++){
		printf(" [%d]-> %d\n",i, R->tuples[i].key );
	}
}

//----------------------------------------------------------------------------------



//XWRAEI MEXRI 5 TUPLES / THA TO ALLAKSOUME
result* store_results(result* result_list, tuple resultR, tuple resultS ){
	//int results=(1024*1024)/8 ; //isws na to pairnei san orisma 
	//results-=3;
	struct result* curr= result_list;
	int count;
	int size=5;//ALLAGH
	if(result_list==NULL){
		result_list=malloc(sizeof(result));
		if (result_list == NULL) { fprintf(stderr, "Malloc failed \n"); return NULL;}
		result_list->tuplesR=malloc(size* sizeof(tuple));
		if (result_list->tuplesR == NULL) { fprintf(stderr, "Malloc failed \n"); return NULL;}

		result_list->tuplesS=malloc(size* sizeof(tuple));
		if (result_list->tuplesS == NULL) { fprintf(stderr, "Malloc failed \n"); return NULL;}
		
		result_list->next=NULL;
		result_list->tuplesR[0]=resultR;
		result_list->tuplesS[0]=resultS;
		result_list->count=1;
		return result_list;

	}
	else{
		while(curr->next!=NULL)curr=curr->next;

		if(curr->count<size){
			count=curr->count;
			curr->tuplesR[count]=resultR;
			curr->tuplesS[count]=resultS;
			curr->count+=1;
			return result_list;
		}
		else if(curr->count==size){
			curr->next=malloc(sizeof(result));
			if (curr->next == NULL) { fprintf(stderr, "Malloc failed \n"); return NULL;}
			curr->next->tuplesR=malloc(size* sizeof(tuple));
			if (curr->next->tuplesR == NULL) { fprintf(stderr, "Malloc failed \n"); return NULL;}
			
			curr->next->tuplesS=malloc(size* sizeof(tuple));
			if (curr->next->tuplesS == NULL) { fprintf(stderr, "Malloc failed \n"); return NULL;}
			
			
			curr->next->tuplesR[0]=resultR;
			curr->next->tuplesS[0]=resultS;

			curr->next->count=1;
			curr->next->next=NULL;
			return result_list;

		}
		else{fprintf(stderr, "Count is wrong \n"); return NULL;}
	}
}

void print_results(result* result_list){
	int count, i, b=0;
	result* curr=result_list;
	printf("--RESULTS--\n");
	if(result_list==NULL)printf("0 results found!\n");
	while(curr!=NULL){
		printf("\n-Bucket: %d\n", b);
		count=curr->count;
		for(i=0; i<count; i++){
			printf(" Key %d Payload R %d, Payload S %d\n", curr->tuplesR[i].key, curr->tuplesR[i].payload, curr->tuplesS[i].payload);
		}
		curr=curr->next;
		b++;
	}
}

// To index kai kala einai ena flag pou mou leei se poio apo ta dyo buckets (pou exei epileksei h compare_buck) exei ginei to index
//kalw thn sunarthsh kai sto orisma tou R vazw to relation pou exei to index enw sto orisma tou S to allo bucket

result* search_results(result* result_list, relation* S_new, int startS, int endS, int** bucket, int** chain, relation* R_new, int hash_size,  int index){ //ston R exei xtistei to eurethrio 
	int i, b,c, hash_val;//  b:bucket, c:chain


	for(i=startS; i<endS; i++){

		hash_val=hash2_func(S_new->tuples[i].key,hash_size);
		if(*bucket[hash_val]!=-1){
			b=*bucket[hash_val];
			c=*chain[b];
			if(R_new->tuples[b].key==S_new->tuples[i].key){ //an to index den einai 1 shmainei oti sthn thesh tou r exoume dwsei to s
				if(index!=0){result_list= store_results(result_list, S_new->tuples[i],   R_new->tuples[b] );} //opote gia na apothhkeusei swsta ta apotelesmata
																												//dinoume anapoda ta orismata 
				else{result_list= store_results(result_list, R_new->tuples[b],  S_new->tuples[i] );}
				//printf("Bucket row %d 	Match 	R:%d = S:%d\n",b, bucketR[b].key, bucketS[i].key );
				while(c!=-1){ //auto deixnei an exei prohgoumeno
					
					if(R_new->tuples[c].key==S_new->tuples[i].key){
						if(index!=0){result_list= store_results(result_list, S_new->tuples[i],  R_new->tuples[c] );}
						else{result_list= store_results(result_list, R_new->tuples[c] , S_new->tuples[i] );}
						//printf("\tChain row %d 	Match 	R:%d = S:%d\n",c,bucketR[c].key, bucketS[i].key );
					}
					else{
						//printf("\tChain Keys do not match: HASH %d and R:%d != S%d\n",hash_val, bucketR[c].key, bucketS[i].key );
					}
					c=*chain[c];
				}

			}
			else{
				//printf("Bucket: Keys do not match: HASH %d and R:%d != S:%d \n",hash_val, bucketR[b].key,bucketS[i].key );
			}

		}
		else{
				//printf("Bucket: -- S:%d does not exist in R!\n", bucketS[i].key );
		}

	}

	return result_list;

} 



int main(){
	
	//int  values[10] = { 0b100100, 0b111001, 0b100001, 0b101101, 0b110100, 0b101101, 0b101001, 0b100101, 0b001101, 0b111100};
	//int  values[10] = { 0b100100, 0b100100, 0b100100,0b100100, 0b100100, 0b100100, 0b100100,0b100100, 0b100100, 0b100100};
	int values[12]={0b10000,0b10000,0b10000,0b10000,0b10000,0b10000,0b10000,0b10000,0b10000,0b10000,0b10000,0b10000 };
	int i, total_bucketsR=0, total_bucketsS=0, hash_size, bucket_size;

	tuple* rel_t;
	rel_t=malloc(12*sizeof(tuple));
	for(i=0; i<12; i++){
		rel_t[i].key=values[i];
		rel_t[i].payload=i;
	}

	relation* R=malloc(sizeof(relation));
	if (R == NULL) { fprintf(stderr, "Malloc failed \n"); return 1;}

	relation* R_new=malloc(sizeof(relation));
	if (R_new == NULL) { fprintf(stderr, "Malloc failed \n"); return 1;}


	R->tuples=rel_t;
	R->num_tuples=12; //allakse to 

	int n=3, hash_val;
	hist_node * R_head = NULL;
	psum_node* phead=NULL;

	phead=NULL;

	for(i =0; i<R->num_tuples; i++){
		hash_val= hash_func( (R->tuples[i].key), n);
		printf("%d, %d\n", R->tuples[i].key, hash_val);
		R_head=update_hist(R_head, hash_val, &total_bucketsR);

	}
	printf("------TOTAL BUCKETS %d\n",total_bucketsR );

	print_hist( R_head);

	phead=create_psumlist( phead, R_head) ;
	//printf("\n\n");
 	//print_psum(phead);

 	R_new=reorder_R(phead,  R,  R_new,  n  );
 	//print_R( R);
 	//printf("\n\n");
 	//print_R(R_new);
 	

 	//-----------------------------------------------------
 	printf("-----Now for relation S\n");
 	//int values2[10]={0b111100, 0b111001, 0b100111, 0b101100, 0b100000, 0b111111, 0b101111, 0b000011, 0b111110, 0b110000};
 	

 	int values2[10]={0b10000, 0b10000, 0b10000, 0b10000, 0b10000, 0b10000, 0b10000, 0b10000, 0b10000, 0b10001};
 	for(i=0; i<10; i++){
		rel_t[i].key=values2[i];
		rel_t[i].payload=10;
	}

	relation* S=malloc(sizeof(relation));
	if (S == NULL) { fprintf(stderr, "Malloc failed \n"); return 1;}

	relation* S_new=malloc(sizeof(relation));
	if (S_new == NULL) { fprintf(stderr, "Malloc failed \n"); return 1;}


	S->tuples=rel_t;
	S->num_tuples=10; //allakse to 

	hist_node* S_head = NULL;
	psum_node* S_phead=NULL;

	for(i =0; i<S->num_tuples; i++){
		hash_val= hash_func( (S->tuples[i].key), n);
		printf("%d, %d\n", S->tuples[i].key, hash_val);
		S_head=update_hist(S_head, hash_val, &total_bucketsS);

	}
	//print_hist( S_head);

	S_phead=create_psumlist( S_phead, S_head) ;
	//printf("\n\n");
 	//print_psum(S_phead);

 	//kanw reorder kai to S
 	S_new=reorder_R(S_phead,  S,  S_new,  n  );
 	//print_R( S);
 	//printf("\n\n");
 	//print_R(S_new);
 	//--------------------------------------------------------------------------------------

	//AUTHAIRETH DOKIMH STO PRWTO BUCKET TOU R

	bucket_size=10;
	hash_size= next_prime(bucket_size);
	printf("Hash size: %d\n",hash_size );

	int** bucket, **chain;
	chain=(int**)malloc(bucket_size* sizeof(int*));
	bucket=malloc(hash_size* sizeof(int*));

	bucket_chain(R_new, 0 , 10, hash_size, bucket, chain);
	printf("CHAIN: {");
	for(i=0; i<bucket_size; i++)printf(" %d",*chain[i]);
	printf("}\nBUCKET: {");
	for(i=0; i<hash_size; i++)printf(" %d",*bucket[i]);
	printf("}\n");
	
	result* result_list=NULL;
	result_list=search_results(result_list, S_new, 0, 12, bucket, chain, R_new, hash_size, 0);
	print_results( result_list);
	


}
