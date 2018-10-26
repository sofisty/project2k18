#include "hash2.h"


int hash2_func(int value,int prime){
	return (value%prime);
}

//elegxei an enas arithmos einai prwtos
int isPrime(int n) {// assuming n > 1
    int i;
    int root;
	 if (n%2 == 0 || n%3 == 0)return 0;
	root = (int)sqrt(n);

    for (i=5; i<=root; i+=6){if (n%i == 0)return 0;}

    for (i=7; i<=root; i+=6){if (n%i == 0)return 0;}

    return 1;
}

//epistrefei ton epomeno prwto arithmo apo ton arithmo pou dinetai ws orisma
int next_prime(int value){
	if(value==0)return 1;
	if(value==1)return 2;

	if(	(value+1)%2==0){value+=2;}
	else {value+=1;}

	while(isPrime(value)!=1)value++;
	return value;
	
	
}


//dhmiourgei ta bucket kai chain
//R_new: taksinomhmeno relation pou periexei to bucket sto opoio ginetai to eurethrio
//start, end : to offset pou ksekinaei to bucket apo to relation R_new
//hash_size : o prwtos arithmos ston opoio moirazontai ta stoixeia tou bucket
int bucket_chain(relation* R_new, int start, int end, int hash_size, int** bucket, int** chain){ 
	int bucket_size=end-start; //posa stoixeia periexei to bucket
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

	//arxikopoiountai oi pinakes bucket, chain me -1
	for(i=0; i<hash_size; i++)*bucket[i]=-1;

	for(i=0; i<bucket_size; i++)*chain[i]=-1;

	int prev, y;
	//printf("bucket size %d\n",bucket_size );

	//gia kathe stoixeio tou bucket 
	for(i=0; i<bucket_size; i++){ //sarwsh tou bucket
		
		//printf("%d\n",R_new->tuples[i+start].key );

		
		y=hash2_func( R_new->tuples[i+start].key,hash_size); //h timh pou antistoixei apo thn hash2

		if(y>=hash_size || y<0){fprintf(stderr, "hash2 value does not match\n"); return 1;}
		if(*bucket[y]==-1){ //an den exei mpei prohgoumenh timh 
			*bucket[y]=i; //vale sthn thesh y(ths hash2) thn thesh  i 
		}
		else{
			prev=*bucket[y]; //vale thn teleutaia grammh pou vrethhke sto bucket
			*bucket[y]=i; 
			*chain[i]=prev; //kai sto chain thn prohgoumenh 
		}
	}
	

	return 0;


} 

void free_bucket_chain(int** bucket, int** chain, int hash_size, int bucket_size){
	int i;
	for(i=0; i<hash_size; i++){free(bucket[i]);}

	for(i=0; i<bucket_size; i++){free(chain[i]);}

	
}

// sygkrinei ta buckets pou dinontai kai epistrefei ena flag gia to mikrotero
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


//Anazhtaei thn antistoixia sta hash values apo 2 istogrammata twn relations R, S. (Kathe hist: taksinomhmenh lista, me auksousa seira hash values)
//enhmerwnei tous deiktes pou kinountai stis listes twn hist kai psum gia thn epomenh sugrish 
/*epeidh einai taksinomhmenes oi listes me ta hash values, arxika krataei thn prwth megaluterh timh kai psaxnei sto hist tou allou relation 
/mexri na vrei antistoixia h kapoia megaluterh timh apo thn zhtoumenh , sthn sunexeia proxwraei tous deiktes gia thn epomenh sugrish*/
int search_match(hist_node** current_R,  hist_node** current_S, psum_node** current_Rp, psum_node** current_Sp ){
	hist_node* curr_R=*current_R, *curr_S=*current_S;
	psum_node* curr_Rp=*current_Rp, *curr_Sp=*current_Sp;

	hist_node* temp; //o deikths tou hist pou kineitai
	psum_node* ptemp;

	int hash_val;
	int flag=0, match=0, greater=1;

	if(curr_R->hash_val>curr_S->hash_val){ //An hash value tou R>S krataw to R kai kineitai to S
				temp=curr_S->next; //kineitai to S kai arxikopoieitai sto epomeno tou
				ptemp=curr_Sp->next;
				hash_val=curr_R->hash_val; //h hash_value pou sugkrinetai
			flag=0;
	}
	else{ //hash val S> R, kineitai to S
		temp=curr_R->next;
		ptemp=curr_Rp->next;
		hash_val=curr_S->hash_val;
		flag=1;
	}

	while(temp!=NULL){ //proxwraei ena apo ta 2 istogrammata analoga to flag
		if(temp->hash_val==hash_val){match=1; break;} //an vrethei megalutero value apo to zhtoumeno hash_value 
		if(temp->hash_val>hash_val){greater=1; break;} // shmainei oti h hash value den uparxei sto hist pou psaxnoume
														
		temp=temp->next;
		ptemp=ptemp->next;
	}

	if(temp==NULL){ //enas apo tous 2 pinakes eftase sto telos kai epeidh einai taksinomhmenoi den uparxei allo match
		printf("There are no other hash matches\n");
		match=-1;
		return match;
	}

	if(match==1){ //an uparxei match epistrefei ta currents gia na dhmiourghthoun ta bucket kai chain
		if(flag==0){//proxwrousa to S ara einai to temp
			//printf("MATCH0%d %d\n",curr_R->hash_val,temp->hash_val);
			
			*current_R=curr_R; 
			*current_Rp=curr_Rp;

			*current_S=temp;
			*current_Sp=ptemp;
			//printf("INSIDE currS->%d curr_R-> %d\n",(*current_S)->hash_val, (*current_R)->hash_val );
		}
		else if (flag==1){//proxwrousa to R ara einai to temp
			//printf("MATCH1 %d %d\n",curr_S->hash_val,temp->hash_val);
			
			*current_S=curr_S;
			*current_Sp=curr_Sp;

			*current_R=temp;
			*current_Rp=ptemp;
			//printf("INSIDE currS->%d curr_R-> %d\n",(*current_S)->hash_val, (*current_R)->hash_val );
		
		}	

		return match;
	}
	else if(match==0 && greater==1){ //an den vrethhke match shmainei oti vrhke kapoia timh megaluterh apo thn hash pou psaxname
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
result*	join(result* result_list, int index,hist_node* curr_R, hist_node* curr_S, psum_node* curr_Rp, psum_node* curr_Sp, relation* R_new, relation* S_new){
	int buck_start, buck_end;
	int hash_size,bucket_size;
	int** bucket=NULL, **chain=NULL;
	if(index==0){ //R exei to hash
		
		buck_start=curr_Rp->offset;
		buck_end=buck_start+curr_R->count;

		hash_size= next_prime(curr_R->count);
		bucket_size=buck_end-buck_start;
		printf(" hash_size %d bucket_size %d start %d end %d\n",hash_size,bucket_size,buck_start,buck_end );

		chain=malloc(bucket_size* sizeof(int*));
		bucket=malloc(hash_size* sizeof(int*));

		//printf("Buck start %d end %d  %d\n",buck_start, buck_end, hash_size );
		bucket_chain(R_new, buck_start , buck_end, hash_size, bucket, chain);
		
		/*printf("CHAIN: {");
		for(int i=0; i<bucket_size; i++)printf(" %d",*chain[i]);
		printf("}\nBUCKET: {");
		for(int i=0; i<hash_size; i++)printf(" %d",*bucket[i]);
		printf("}\n");
		*/
		
		//ftiaxnei thn lista me tous buffers
		result_list=search_results(result_list, S_new, curr_Sp->offset, (curr_Sp->offset+curr_S->count), bucket, chain, R_new, curr_Rp->offset, hash_size, index);
		free_bucket_chain(bucket, chain,hash_size, bucket_size);

		
	}
	else if(index==1){ //O S exei to hash
	
		buck_start=curr_Sp->offset;
		buck_end=buck_start+curr_S->count;

		hash_size= next_prime(curr_S->count);
		bucket_size=buck_end-buck_start;
		printf(" hash_size %d bucket_size %d start %d end %d\n",hash_size,bucket_size,buck_start,buck_end );


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
		fprintf(stderr, "Wrong value for index\n");
		return NULL;
	}

	free(bucket);
	free(chain);
	return result_list;
}


//Sarwnei ola ta buckets kai kalei tis sunarthseis gia th dhmiourgia twn bucket,chain kai thn anazhthsh twn apotelesmatwn 
int final_hash(hist_node* R_head, hist_node* S_head,psum_node* R_phead,psum_node* S_phead, relation* R_new, relation* S_new){
	
	result* result_list=NULL;
		
	hist_node *curr_R,*curr_S;
	psum_node *curr_Rp,*curr_Sp;
	curr_R=R_head; //to prwto stoixeio tou hist tou R
	curr_S=S_head;

	curr_Rp=R_phead; //to prwto stoixeio tou psum tou R
	curr_Sp=S_phead;
	int match, index; //match: flag an 1:uparxei match, 0: alliws, index: 0: an sto R ginetai hash2, 1:alliws
	
	
	int i;
	
	//gia kathe node apo tis listes hist twn relations
	while(((curr_R)!=NULL)&&((curr_S)!=NULL)){
		
		if(curr_R->hash_val==curr_S->hash_val){ //an ta hash_values einai idia
			//printf("MATCH %d %d\n",curr_R->hash_val,curr_S->hash_val);
			index=buck_compare( curr_R, curr_S, curr_Rp, curr_Sp); //vres se poio relation tha ginei to hash
			result_list=join(result_list,  index, curr_R, curr_S, curr_Rp, curr_Sp, R_new, S_new); //epestrepse ta apotelesmata
			 
			//proxwra kai ta 2 hist ston epomeno komvo
			curr_R=curr_R->next;
			curr_Rp=curr_Rp->next;
			
			curr_S=curr_S->next;
			curr_Sp=curr_Sp->next;
		}
		else{ 
			//printf(" curr_R->key: %d curr_S->key: %d\n",curr_R->hash_val, curr_S->hash_val );
			match=search_match(&curr_R, &curr_S, &curr_Rp, &curr_Sp ); //vres an uparxei kapoios komvos epomenos me match kai enhmerwse tous deiktes
			//printf("NEW  curr_R->key: %d curr_S->key: %d\n",curr_R->hash_val, curr_S->hash_val );
			
			if(match==-1){
				return -1;
			}
			else if( match==1){ //an vrethhke kapoio match , psakse gia ta apotelesmata
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


