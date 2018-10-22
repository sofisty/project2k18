#include "hash2.h"


int hash2_func(int value,int prime){
	return (value%prime);
}


int isPrime(int n) {// assuming n > 1
    int i;
    int root;
	 if (n%2 == 0 || n%3 == 0)return 0;
	root = (int)sqrt(n);

    for (i=5; i<=root; i+=6){if (n%i == 0)return 0;}

    for (i=7; i<=root; i+=6){if (n%i == 0)return 0;}

    return 1;
}


int next_prime(int value){
	if(value==0)return 1;
	if(value==1)return 2;

	if(	(value+1)%2==0){value+=2;}
	else {value+=1;}

	while(isPrime(value)!=1)value++;
	return value;
	
	
}



int bucket_chain(relation* R_new, int start, int end, int hash_size, int** bucket, int** chain){ //tha kaleitai gia kathe bucket pou exei ginei eurethrio
																								// hash_size einai o next prime			
	int  i;
	int bucket_size=end-start;
	
	for(i=0; i<hash_size; i++){
		bucket[i]=malloc(sizeof(int));
		if (bucket[i] == NULL) { fprintf(stderr, "Malloc failed \n"); return 1;}
	}

	for(i=0; i<10; i++){
		chain[i]=malloc(sizeof(int));
		if (chain[i] == NULL) { fprintf(stderr, "Malloc failed \n"); return 1;}
	}
	
	for(i=0; i<hash_size; i++)*bucket[i]=-1;
	for(i=0; i<bucket_size; i++)*chain[i]=-1;
	
	int prev, y;
	for(i=start; i<end; i++){ //sarwsh tou eurethriou
		y=hash2_func( R_new->tuples[i].key,hash_size);

		if(y>=hash_size || y<0){fprintf(stderr, "hash2 value does not match\n"); return 1;}
		if(*bucket[y]==-1){ //an den exei mpei prohgoumenh timh 
			*bucket[y]=i; //vale sthn thesh y(bucket y ths hash2) thn thesh  i 
		}
		else{
			prev=*bucket[y];
			*bucket[y]=i;
			*chain[i]=prev;
		}
	}
	

	return 0;


} 

int buck_compare( hist_node* R_head, hist_node* S_head,psum_node* R_phead,psum_node* S_phead, int* buck_start,int* buck_end,int hash_val){
	if(R_head->count>S_head->count){
		printf("To bucket tou R exei exei %d tuples enw to bucket tou S exei %d tuples.\n",R_head->count,S_head->count);
		printf("Tha kanw hash to S\n");
		*buck_start=search_offset(S_phead,hash_val);
		*buck_end=(*buck_start)+(S_head->count);
		return 1;
	} 
	else{
		printf("To bucket tou R exei exei %d tuples enw to bucket tou S exei %d tuples.\n",R_head->count,S_head->count);
		printf("Tha kanw hash to R\n");
		*buck_start=search_offset(R_phead,hash_val);
		*buck_end=(*buck_start)+(R_head->count);
		return 0;
	} 
}

int search_offset(psum_node* phead,int hash_val){
	psum_node* curr=phead;
	int offset;
	while(curr!=NULL){
		if(curr->hash_val==hash_val){
			offset=curr->curr_off;
			curr->curr_off++;
			printf("Epistrefw offset: %d\n\n",offset );
			return offset;
		}
		curr=curr->next;
	}
	printf("Kati phge lathos!\n");
	return -1;

}

int final_hash(hist_node* R_head, hist_node* S_head,psum_node* R_phead,psum_node* S_phead){
	int i,numofhvR,numofhvS,buck_size,B_size,hash_val,toHash,*buck_start,*buck_end; //B_size=megethos tou pinaka Bucket 
	numofhvS=0;																				 //pou tha ftiaxtei sti sinexeia
	numofhvR=0;
	hist_node* curr_R=R_head;
	hist_node* curr_S=S_head; 

	buck_start=malloc(sizeof(int));
	if (buck_start == NULL) { fprintf(stderr, "Malloc failed \n"); return 1;}
	buck_end=malloc(sizeof(int));
	if (buck_end == NULL) { fprintf(stderr, "Malloc failed \n"); return 1;}


	//metraw posa hash values exei to kathe relation
	while(curr_R!=NULL){
		numofhvR++;
		curr_R=curr_R->next;
	}
	while(curr_S!=NULL){
		numofhvS++;
		curr_S=curr_S->next;
	}

	curr_R=R_head;
	curr_S=S_head;

	if(numofhvR<numofhvS){
		while(curr_R!=NULL){
			while(curr_S!=NULL){
				if(curr_R->hash_val==curr_S->hash_val){
					hash_val=curr_R->hash_val;
					printf("Vrethike koino hashvalue: %d\n\n",curr_R->hash_val);
					toHash=buck_compare(curr_R,curr_S,R_phead,S_phead,buck_start,buck_end,hash_val);
					buck_size=(*buck_end)-(*buck_start);
					B_size=next_prime(buck_size);
					printf("O pinakas Bucket tha exei megethos: %d\n\n",B_size);

					for(i=(*buck_start);i<(*buck_end);i++){
						
					}
					break;
				}
				else curr_S=curr_S->next;
			}
			curr_R=curr_R->next;
			curr_S=S_head;			
		}
	}
	else{
		while(curr_S!=NULL){
			while(curr_R!=NULL){
				if(curr_S->hash_val==curr_R->hash_val){
					hash_val=curr_R->hash_val;
					printf("Vrethike koino hashvalue: %d\n\n",curr_S->hash_val);
					toHash=buck_compare(curr_R,curr_S,R_phead,S_phead,buck_start,buck_end,hash_val);
					buck_size=(*buck_end)-(*buck_start);
					B_size=next_prime(buck_size);
					printf("O pinakas Bucket tha exei megethos: %d\n\n",B_size);


					for(i=(*buck_start);i<(*buck_end);i++){
						
					}
					break;
				}
				else curr_R=curr_R->next;
			}
			curr_S=curr_S->next;
			curr_R=R_head;			
		}

	}
	free(buck_start);
	free(buck_end);
	return 0;
}


