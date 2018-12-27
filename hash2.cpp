#include "hash2.h"


int hash2_func(uint64_t value,int prime){
	return ((int)value%prime);
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
		bucket[i]=(int*)malloc(sizeof(int));
		if (bucket[i] == NULL) { fprintf(stderr, "Malloc failed \n"); return 1;}
	}

	for(i=0; i<bucket_size; i++){
		chain[i]=(int*)malloc(sizeof(int));
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
int buck_compare( hist_node R_hist, hist_node S_hist){
	if(R_hist.count>S_hist.count){
		//printf("-Hash S: R= %d tuples / S= %d tuples.\n",R_head->count,S_head->count);		
		return 1;
	} 
	else{
		//printf("-Hash R: R= %d tuples / S= %d tuples.\n",R_head->count,S_head->count);		
		return 0;
	} 
}





//KALEI TA BUCKET CHAIN KAI THN ANAZHTHSH TWN APOTELESMATWN GIA TIS 2 PERIPTWSEIS R:HASH2 H S:HASH2
result*	join( result** head, result* curr_res, int index, hist_node curr_R, hist_node curr_S, psum_node curr_Rp, psum_node curr_Sp, relation* R_new, relation* S_new){
	int buck_start, buck_end;
	int hash_size,bucket_size;
	int** bucket=NULL, **chain=NULL;
	if(index==0){ //R exei to hash
		
		buck_start=curr_Rp.offset;
		buck_end=buck_start+curr_R.count;

		hash_size= next_prime(curr_R.count);
		bucket_size=buck_end-buck_start;
		//printf(" hash_size %d bucket_size %d start %d end %d\n",hash_size,bucket_size,buck_start,buck_end );

		chain=(int**)malloc(bucket_size* sizeof(int*));
		bucket=(int**)malloc(hash_size* sizeof(int*));
		//printf("Buck start %d end %d  %d\n",buck_start, buck_end, hash_size );
		bucket_chain(R_new, buck_start , buck_end, hash_size, bucket, chain);

		//ftiaxnei thn lista me tous buffers
		curr_res=search_results(head,curr_res, S_new, curr_Sp.offset, (curr_Sp.offset+curr_S.count), bucket, chain, R_new, curr_Rp.offset, hash_size, index);
		free_bucket_chain(bucket, chain,hash_size, bucket_size);

		
	}
	else if(index==1){ //O S exei to hash
	
		buck_start=curr_Sp.offset;
		buck_end=buck_start+curr_S.count;

		hash_size= next_prime(curr_S.count);
		bucket_size=buck_end-buck_start;
		//printf(" hash_size %d bucket_size %d start %d end %d\n",hash_size,bucket_size,buck_start,buck_end );

		chain=(int**)malloc(bucket_size* sizeof(int*));
		bucket=(int**)malloc(hash_size* sizeof(int*));

		bucket_chain(S_new, buck_start , buck_end, hash_size, bucket, chain);

		curr_res=search_results(head, curr_res, R_new, curr_Rp.offset, (curr_Rp.offset+curr_R.count), bucket, chain, S_new, curr_Sp.offset, hash_size, index);
		free_bucket_chain(bucket, chain,hash_size, bucket_size);
	}
	else{
		fprintf(stderr, "Wrong value for index\n");
		return NULL;
	}

	free(bucket);
	free(chain);
	return curr_res;
}


//Sarwnei ola ta buckets kai kalei tis sunarthseis gia th dhmiourgia twn bucket,chain kai thn anazhthsh twn apotelesmatwn 
result* final_hash(hist_node* R_hist, hist_node* S_hist,psum_node* R_psum,psum_node* S_psum, relation* R_new, relation* S_new){
	int i;
	result* result_list=NULL;
	result* curr_res=result_list;	

	int  index; //match: flag an 1:uparxei match, 0: alliws, index: 0: an sto R ginetai hash2, 1:alliws
	
	
	//gia kathe node apo tous pinakes hist twn relations
	for(i=0; i<256; i++){
		if(R_hist[i].count==0 || S_hist[i].count==0)continue;
		index=buck_compare( R_hist[i],S_hist[i]);
		curr_res=join(&result_list ,curr_res, index, R_hist[i], S_hist[i], R_psum[i], S_psum[i], R_new, S_new); //epestrepse ta apotelesmata
	}

	int num_results=0;
	
	
	return result_list;
}


