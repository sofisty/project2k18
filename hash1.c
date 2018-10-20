#include "hash1.h"

int hash_func(int value, int n){
	int l=1;
	for(int i =0; i<n; i++) l*=2;
	//printf("l is %d\n",l ); 
	return (value%l);
}


hist_node*  update_hist(hist_node* head, int hash_val){
	if(head==NULL){
		head = malloc(sizeof(hist_node));
		if (head == NULL) {
	    fprintf(stderr, "Malloc failed \n"); return NULL;
		}

		head->hash_val = hash_val;
		head->count=1;
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
	int  offset, key;
	//printf(" SIZE %d\n",size );
	R_new->num_tuples=size;
	R_new->tuples=malloc(size* sizeof(tuple));
	if(R_new->tuples==NULL){ fprintf(stderr, "Malloc failed \n"); return NULL;}

	for(i=0; i<size; i++){

		key=R->tuples[i].key;
		offset=search_Psum( phead, key, n );
		//print_psum( phead);
		R_new->tuples[offset].key=key;


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


//NA DEIS TO SONG OF THE SEA!!


int bucket_chain(tuple* tuples,int bucket_size, int hash_size, int** bucket, int** chain){ //tha kaleitai gia kathe bucket pou exei ginei eurethrio
																						//bucket size =oi eggrafes pou exei to bucket prin thn hash2 
																						//hash size = to range ths hash2 (prwtos)			
	int  i;

	//bucket=malloc(sizeof(int*));
	//*bucket=malloc(hash_size* sizeof(int));
	//if (bucket == NULL) { fprintf(stderr, "Malloc failed \n"); return 1;}
	
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
	for(i=0; i<bucket_size; i++){ //sarwsh tou eurethriou
		y=tuples[i].key; //y=kai kala h hash value sth thesh i  
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


int main(){
	
	int  values[10] = { 0b100100, 0b111001, 0b100001, 0b101101, 0b110100, 0b101101, 0b101001, 0b100101, 0b001101, 0b111100};
	//int  values[10] = { 0b100100, 0b100100, 0b100100,0b100100, 0b100100, 0b100100, 0b100100,0b100100, 0b100100, 0b100100};
	int i;

	tuple* rel_t;
	rel_t=malloc(10*sizeof(tuple));
	for(i=0; i<10; i++){
		rel_t[i].key=values[i];
		rel_t[i].payload=10;
	}

	relation* R=malloc(sizeof(relation));
	if (R == NULL) { fprintf(stderr, "Malloc failed \n"); return 1;}

	relation* R_new=malloc(sizeof(relation));
	if (R_new == NULL) { fprintf(stderr, "Malloc failed \n"); return 1;}


	R->tuples=rel_t;
	R->num_tuples=10; //allakse to 

	int n=3, hash_val;
	hist_node * head = NULL;
	psum_node* phead=NULL;

	phead=NULL;
	


	for(i =0; i<R->num_tuples; i++){
		hash_val= hash_func( (R->tuples[i].key), n);
		printf("%d, %d\n", R->tuples[i].key, hash_val);
		head=update_hist(head, hash_val);

	}

	print_hist( head);

	phead=create_psumlist( phead, head) ;
	printf("\n\n");
 	print_psum(phead);

 	R_new=reorder_R(phead,  R,  R_new,  n  );
 	print_R( R);
 	printf("\n\n");
 	print_R(R_new);

 	//proswrina tuples kai kala oi hash2 tou bucket 0
 	int  values1[10] = {1,2,5,5,10,2,7,1,1,3};
 	//int  values1[10] = {1,1,1,1,1,1,1,1,1,1};
 	//int  values1[10] = {1,2,3,4,5,6,7,8,9,10};
 	
 	
 	//swsta apotelesmata: bucket{-1,8,5,9,-1,3,-1,6,-1,3,4} gia to prwto
 						//chain{-1,-1,-1,2,-1,1,-1,0,7,-1}					

 	for(i=0; i<10; i++){
		rel_t[i].key=values1[i];
		rel_t[i].payload=10;
	}
	
	 //fantazomai auta tha ginoun mesa sthn sunarthsh me thn hash2

	int bucket_size=10, hash_size=11;
	int** bucket, **chain;
	chain=(int**)malloc(bucket_size* sizeof(int*));
	bucket=malloc(hash_size* sizeof(int*));
	

	bucket_chain( rel_t,bucket_size, hash_size  ,bucket,  chain);
	printf("CHAIN: {");
	for(i=0; i<bucket_size; i++)printf(" %d",*chain[i]);
	printf("}\nBUCKET: {");
	for(i=0; i<hash_size; i++)printf(" %d",*bucket[i]);
		printf("}\n");

	return 0;

}
