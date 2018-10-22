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
			//printf("Epistrefw offset: %d\n\n",offset );
			return offset;
		}
		curr=curr->next;
	}
	printf("Kati phge lathos!\n");
	return -1;

}

//

int search_match(hist_node* bigger,hist_node* smaller,hist_node* moving){
	moving=smaller;
	while((((moving)->hash_val)<bigger->hash_val)&&(moving!=NULL)){
		(moving)=(moving)->next;
	}
	if((moving)==NULL){		
		printf("No matching hash values where found.\n");
		return 0;
	}
	else{
		printf("Match found, hash value: %d\n", (moving)->hash_val);
		return 1;
	}
}

//final_hash(R_head,S_head,phead,S_phead);


//THN EXW ALLAKSEI ARKETA , LEIPOUN OMWS PALI TA COMPARE BUCKETS DEN TA XW VALEI
//kai vasika oles tis sunarthseis tis evala mesa an den s aresei ennoeitai allakse to olo 
//apla ayto skefthka ekeinh thn stigmh 
//douleuei komple mou fainetai

int final_hash(hist_node* R_head, hist_node* S_head,psum_node* R_phead,psum_node* S_phead){
	
	//ta sxoliasa gia ta warnings
	//int i,buck_size,B_size,toHash,buck_start,buck_end; //B_size=megethos tou pinaka Bucket 
	
	int hash_val;
	hist_node *curr_R,*curr_S;//, *start_point;
	psum_node *curr_Rp,*curr_Sp;//, *start_pointP;

	hist_node* temp; //ayto pou kineitai
	psum_node* ptemp;

	curr_R=R_head; //currents
	curr_S=S_head;

	curr_Rp=R_phead;
	curr_Sp=S_phead;

	int flag=0, match=0, greater=1;

	while(((curr_R)!=NULL)&&((curr_S)!=NULL)){
		

		if(curr_R->hash_val==curr_S->hash_val){
			printf("VRHKA MATCH %d %d\n",curr_R->hash_val,curr_S->hash_val);
			//COMPARE BUCKETS KAI BUCKET CHAIN
			curr_R=curr_R->next;
			curr_Rp=curr_Rp->next;
			
			curr_S=curr_S->next;
			curr_Sp=curr_Sp->next;
		}
		else{ 
			
			if(curr_R->hash_val>curr_S->hash_val){ //AN HASH_VAL TOU R>S krataw to R kai kineitai to S
				temp=curr_S->next; //kineitai to S kai arxikopoieitai sto epomeno tou
				ptemp=curr_Sp->next;
				hash_val=curr_R->hash_val; //h hash_value pou sugkrinetai
				flag=0;
			}
			else{ //HASH_VAL S> R kineitai to S
				temp=curr_R->next;
				ptemp=curr_Rp->next;
				hash_val=curr_S->hash_val;
				flag=1;
			}

			while(temp!=NULL){ //proxwraei ena apo ta 2 istogrammata analoga to flag
				if(temp->hash_val==hash_val){match=1; break;} 
				if(temp->hash_val>hash_val){greater=1; break;} //an vrethei megalutero value apo to zhtoumeno hash_value 
																// shmainei oti den uparxei sto hist pou psaxnoume
				temp=temp->next;
				ptemp=ptemp->next;
			}

			if(temp==NULL){ //enas apo tous 2 pinakes eftase sto telos kai epeidh einai taksinomhmenoi den uparxei allo match
				printf("There are no other hash matches\n");
				break;
			}

			if(match==1){ //an uparxei match
				if(flag==0){//proxwrousa to S ara to epomeno stoixeio tou tha einai to temp->next
					printf("MATCH %d %d\n",curr_R->hash_val,temp->hash_val);
					//COMPARE BUCKETS KAI BUCKET CHAIN meta curr_R, curr_Rp klp klp
					
					curr_R=curr_R->next; 
					curr_Rp=curr_Rp->next;

					curr_S=temp->next;
					curr_Sp=ptemp->next;
				}
				else if (flag==1){//proxwrousa to R ara to epomeno stoixeio tou tha einai to temp->next
					printf("MATCH %d %d\n",curr_S->hash_val,temp->hash_val);
					//COMPARE BUCKETS KAI BUCKET CHAIN meta curr_R, curr_Rp klp klp
					curr_S=curr_S->next;
					curr_Sp=curr_Sp->next;

					curr_R=temp->next;
					curr_Rp=ptemp->next;
				
				}
				
				match=0; 
			}
			else if(match==0 && greater==1){ //an den vrethhke match shmainei oti vrhke kapoia timh megaluterh apo thn hash pou anazhtousame 
								//kai tha ginei h epomenh hash_value pou anazhtoume  

				if(flag==0){ //proxwrouse o S
					curr_S=temp; //kai ara stamathse o temp se mia timh > hash_value opou ginetai h current
					curr_Sp=ptemp;
					curr_R=curr_R->next;
					curr_Rp=curr_Rp->next;
				}
				else if (flag==1){ //antistoixa gia ton R
					curr_R=temp;
					curr_Rp=ptemp;
					curr_S=curr_S->next;
					curr_Sp=curr_Sp->next;
				}
			}

		

		}
		
		
	}


	
		

		return 0;
}


