#include "hash1.h"
#include "hash2.h"



int hash_func(uint64_t value, int n){ //hash function gia thn tmhmatopoish , dimiourgia evretiriwn sta R kai S	
	int last8;
   	
   	last8 = value & ((1L<<n)-1);
	if(last8>=256 || last8<0){
		fprintf(stderr, "Wrong hash value %d\n",last8 );
		return -1;
	}
	
	return last8;
}



hist_node* update_hist(hist_node* hist, relation* R, int n){
	int i;
	int hash_val; 
	for(i =0; i<R->num_tuples; i++){ //pernaw apo ton tuplesR tis eggrafes sti domi tou relation
		hash_val= hash_func( (R->tuples[i].key), n);
		if(hash_val<0)return NULL;
		hist[hash_val].count+=1;
		

	}
	return hist;
}

void print_hist(hist_node* hist) { //typwnei to istogramma sth parousa katastash sou
   if(hist!=NULL){
   		for(int i=0; i<256; i++){printf("Hash value %d has %d records\n",i, hist[i].count );}
   	}
  	
}




psum_node* update_psumlist(psum_node* psum, hist_node* hist ){ //dhmiourgei thn lista me ta psum
	 int offset=0, i;
	
	 if(psum==NULL)return NULL;

	 for(i=0; i<256; i++){
	 	psum[i].hash_val=hist[i].hash_val;
	 	psum[i].offset=offset;
	 	psum[i].curr_off=offset;
	 	offset+=hist[i].count;	
	 }

   return psum; //epistrefei tin kefalh ths listas

}


void print_psum(psum_node * psum) { //ektypwnei tin psum lista
    for(int i =0; i< 256; i++){
    	printf("Psum with hash value %d and %d offset\n",psum[i].hash_val, psum[i].offset );
    }
}


relation* reorder_R(psum_node* phead, relation* R, relation* R_new, int n ){ //kanei reorder na relation me vasi thn psum list
	int i,y, size=R->num_tuples, curr_off;
	int  key, payload;
	R_new->num_tuples=size;
	R_new->tuples=malloc(size* sizeof(tuple));
	if(R_new->tuples==NULL){ fprintf(stderr, "Malloc failed \n"); return NULL;}

	for(i=0; i<size; i++){ //ftiaxnei to R_new pou tha epistrafei me thn seira etsi wste na armozei h tmhmatopoihsh tou
						   // me auth thn psum kai tou isogrammatos
		key=R->tuples[i].key;
		payload=R->tuples[i].payload;
		y=hash_func(key, n);
		curr_off=phead[y].curr_off;
		phead[y].curr_off+=1;

		R_new->tuples[curr_off].key=key;
		R_new->tuples[curr_off].payload=payload;
	}
	return R_new; //epistrefei th nea anadiorganwmenh sxesh

}

void print_R(relation* R){ //ektipwnei ta tuples ths relation pou dinetai
	int i, h, size=R->num_tuples;
	printf("Relation num_tuples: %d\n",size );
	for(i=0; i<size; i++){
		printf(" [%d]->  key %ld , payload %ld \n",i, R->tuples[i].key,R->tuples[i].payload );
	}
}

void free_R(relation* R){
	if(R!=NULL){
	free(R->tuples);
	free(R);
	}
	

}


