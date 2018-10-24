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


//EIXA KSEXASEI TELEIWS TA OFFSETS
//H fash einai oti to chain exei antiatoixia me tis grammes san to bucket na ksekinouse apo 0
//opote gia na anazhthsoume kati sta bucket-chain apla prosthetoume to offset tou bucket
int bucket_chain(relation* R_new, int start, int end, int hash_size, int** bucket, int** chain){ 
	int bucket_size=end-start;
	int i;
	for(i=0; i<hash_size; i++){
		bucket[i]=malloc(sizeof(int));
		if (bucket[i] == NULL) { fprintf(stderr, "Malloc failed \n"); return 1;}
	}

	for(i=0; i<bucket_size; i++){
		chain[i]=malloc(sizeof(int));
		if (chain[i] == NULL) { fprintf(stderr, "Malloc failed \n"); return 1;}
	}
	//printf("hash %d bucket %d\n",hash_size,bucket_size );

	for(i=0; i<hash_size; i++)*bucket[i]=-1;

	for(i=0; i<bucket_size; i++)*chain[i]=-1;

	int prev, y;
	//printf("bucket size %d\n",bucket_size );
	for(i=0; i<bucket_size; i++){ //sarwsh tou eurethriou
		
		//printf("%d\n",R_new->tuples[i+start].key ); 

		y=hash2_func( R_new->tuples[i+start].key,hash_size); //-----AUTO ENNOW!!-----

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

void free_bucket_chain(int** bucket, int** chain, int hash_size, int bucket_size){
	int i;
	for(i=0; i<hash_size; i++){free(bucket[i]);}

	for(i=0; i<bucket_size; i++){free(chain[i]);}

	
}

//apla epistrefei to flag giati telika ta start end htan kalutero na upologizontai ekei pou ginetai to bucket chain
int buck_compare( hist_node* R_head, hist_node* S_head,psum_node* R_phead,psum_node* S_phead){
	if(R_head->count>S_head->count){
		printf("-Hash S: R= %d tuples / S= %d tuples.\n",R_head->count,S_head->count);		
		return 1;
	} 
	else{
		printf("-Hash R: R= %d tuples / S= %d tuples.\n",R_head->count,S_head->count);		
		return 0;
	} 
}


//akrivws ta ifs 
int search_match(hist_node** current_R,  hist_node** current_S, psum_node** current_Rp, psum_node** current_Sp ){
	hist_node* curr_R=*current_R, *curr_S=*current_S;
	psum_node* curr_Rp=*current_Rp, *curr_Sp=*current_Sp;

	hist_node* temp; //ayto pou kineitai
	psum_node* ptemp;

	int hash_val;
	int flag=0, match=0, greater=1;

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
		match=-1;
		return match;
	}

	if(match==1){ //an uparxei match epistrefei ta current gia na ginoun ta bucket chain
		if(flag==0){//proxwrousa to S ara to epomeno stoixeio tou tha einai to temp->next
			//printf("MATCH0%d %d\n",curr_R->hash_val,temp->hash_val);
			
			*current_R=curr_R; 
			*current_Rp=curr_Rp;

			*current_S=temp;
			*current_Sp=ptemp;

			//printf("INSIDE currS->%d curr_R-> %d\n",(*current_S)->hash_val, (*current_R)->hash_val );
		}
		else if (flag==1){//proxwrousa to R ara to epomeno stoixeio tou tha einai to temp->next
			//printf("MATCH1 %d %d\n",curr_S->hash_val,temp->hash_val);
			
			*current_S=curr_S;
			*current_Sp=curr_Sp;

			*current_R=temp;
			*current_Rp=ptemp;
			//printf("INSIDE currS->%d curr_R-> %d\n",(*current_S)->hash_val, (*current_R)->hash_val );
		
		}	

		return match;
	}
	else if(match==0 && greater==1){ //an den vrethhke match shmainei oti vrhke kapoia timh megaluterh apo thn hash pou anazhtousame 
						//kai tha ginei h epomenh hash_value pou anazhtoume  

		if(flag==0){ //proxwrouse o S
			*current_S=temp; //kai ara stamathse o temp se mia timh > hash_value opou ginetai h current
			*current_Sp=ptemp;
			*current_R=curr_R->next;
			*current_Rp=curr_Rp->next;
		}
		else if (flag==1){ //antistoixa gia ton R
			*current_R=temp;
			*current_Rp=ptemp;
			*current_S=curr_S->next;
			*current_Sp=curr_Sp->next;
		}

		return match;
	}


}


//KALEI TA BUCKET CHAIN KAI THN ANAZHTHSH TWN APOTELESMATWN GIA TIS 2 PERIPTWSEIS R:HASH2 H S:HASH2
// PS kai ekei eixa ksexasei to offset tou bucket gia thn sugrish sta results  opote pairnei kai to offest h search results
result*	join(result* result_list, int index,hist_node* curr_R, hist_node* curr_S, psum_node* curr_Rp, psum_node* curr_Sp, relation* R_new, relation* S_new){
	int buck_start, buck_end;
	int hash_size,bucket_size;
	int** bucket=NULL, **chain=NULL;
	if(index==0){ //R exei to hash
		
		buck_start=curr_Rp->offset;
		buck_end=buck_start+curr_R->count;

		hash_size= next_prime(curr_R->count);
		bucket_size=buck_end-buck_start;
		//printf(" hash_size %d bucket_size %d start %d end %d\n",hash_size,bucket_size,buck_start,buck_end );

		chain=malloc(bucket_size* sizeof(int*));
		bucket=malloc(hash_size* sizeof(int*));

		//printf("Buck start %d end %d  %d\n",buck_start, buck_end, hash_size );
		bucket_chain(R_new, buck_start , buck_end, hash_size, bucket, chain);
		/*
		printf("CHAIN: {");
		for(int i=0; i<bucket_size; i++)printf(" %d",*chain[i]);
		printf("}\nBUCKET: {");
		for(int i=0; i<hash_size; i++)printf(" %d",*bucket[i]);
		printf("}\n");
		*/

		result_list=search_results(result_list, S_new, curr_Sp->offset, (curr_Sp->offset+curr_S->count), bucket, chain, R_new, curr_Rp->offset, hash_size, index);
		free_bucket_chain(bucket, chain,hash_size, bucket_size);

		
	}
	else if(index==1){
	
		buck_start=curr_Sp->offset;
		buck_end=buck_start+curr_S->count;

		hash_size= next_prime(curr_S->count);
		bucket_size=buck_end-buck_start;
		//printf(" hash_size %d bucket_size %d start %d end %d\n",hash_size,bucket_size,buck_start,buck_end );


		chain=malloc(bucket_size* sizeof(int*));
		bucket=malloc(hash_size* sizeof(int*));

		bucket_chain(S_new, buck_start , buck_end, hash_size, bucket, chain);
		/*printf("CHAIN: {");
		for(int i=0; i<bucket_size; i++)printf(" %d",*chain[i]);
		printf("}\nBUCKET: {");
		for(int i=0; i<hash_size; i++)printf(" %d",*bucket[i]);
		printf("}\n");
		*/

		result_list=search_results(result_list, R_new, curr_Rp->offset, (curr_Rp->offset+curr_R->count), bucket, chain, S_new, curr_Sp->offset, hash_size, index);
		free_bucket_chain(bucket, chain,hash_size, bucket_size);
	}
	else{
		printf("WHAAAAAAT\n");
	}

	free(bucket);
	free(chain);
	return result_list;
}


int final_hash(hist_node* R_head, hist_node* S_head,psum_node* R_phead,psum_node* S_phead, relation* R_new, relation* S_new){
	
	result* result_list=NULL;
		
	hist_node *curr_R,*curr_S;//, *start_point;
	psum_node *curr_Rp,*curr_Sp;//, *start_pointP;

	curr_R=R_head; //currents
	curr_S=S_head;

	curr_Rp=R_phead;
	curr_Sp=S_phead;
	int match, index;
	
	
	int i;
	
	while(((curr_R)!=NULL)&&((curr_S)!=NULL)){
		
		if(curr_R->hash_val==curr_S->hash_val){
			//printf("MATCH %d %d\n",curr_R->hash_val,curr_S->hash_val);
			index=buck_compare( curr_R, curr_S, curr_Rp, curr_Sp);
			result_list=join(result_list,  index, curr_R, curr_S, curr_Rp, curr_Sp, R_new, S_new);
	

			curr_R=curr_R->next;
			curr_Rp=curr_Rp->next;
			
			curr_S=curr_S->next;
			curr_Sp=curr_Sp->next;
		}
		else{ 
			//printf(" curr_R->key: %d curr_S->key: %d\n",curr_R->hash_val, curr_S->hash_val );
			match=search_match(&curr_R, &curr_S, &curr_Rp, &curr_Sp );
			//printf("NEW  curr_R->key: %d curr_S->key: %d\n",curr_R->hash_val, curr_S->hash_val );
			
			if(match==-1){
				break;
			}
			else if( match==1){
				index=buck_compare( curr_R, curr_S, curr_Rp, curr_Sp);
				result_list=join(result_list,  index, curr_R, curr_S, curr_Rp, curr_Sp, R_new, S_new);

				curr_R=curr_R->next;
				curr_Rp=curr_Rp->next;
				
				curr_S=curr_S->next;
				curr_Sp=curr_Sp->next;			

				
			}		

		}
		
		
	}

	print_results( result_list);
	free_result_list( result_list);	

	return 0;
}


