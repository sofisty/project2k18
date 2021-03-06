#include "results.h"
#include "jobScheduler.h"
#define NUM_THREADS 8
#define n 8
#define m (1<<n)

#define size (128*1024)/(3*8) //arithmos eggrafwn pou xwrane sto ena bucket ths listas 
//#define size 2000000000

using namespace std;

result* store_results( result** head,result* curr_res, tup resultR, tup resultS ){ //apothikevei enan sindiasmo tuples sthn lista apotelesmatwn

	int count,index;

	//printf("SIZE %d\n",size );
	
	if( curr_res==NULL){//an exei dothei keni lista tote dimiourgw kainourgia lista apotelesmatwn
		 curr_res=(result*)malloc(sizeof(result));
		if (curr_res == NULL) { fprintf(stderr, "Malloc failed \n"); return NULL;}
		curr_res->matches=(uint64_t*)malloc(size*3* sizeof(uint64_t));
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
			 
			curr_res->next=(result*)malloc(sizeof(result));
			if (curr_res->next == NULL) { fprintf(stderr, "Malloc failed \n"); return NULL;}
			curr_res->next->matches=(uint64_t*)malloc(size*3* sizeof(uint64_t));
			if (curr_res->next->matches== NULL) { fprintf(stderr, "Malloc failed \n"); return NULL;}
			
			
			
			curr_res->next->matches[0]=resultR.payload; //kai arxikopoiw thn lista apotelesmatwn me ta dothenta tuples
			curr_res->next->matches[1]=resultS.payload;
			curr_res->next->matches[2]=resultR.key;

			curr_res->next->count=1; //auksanw kata ena to count tou neou bucket
			curr_res->next->next=NULL;
			
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
		printf("\n-Bucket: %d\n", b);

		count=curr->count;
		for(i=0; i<count; i++){
			total++;
			index=3*i;
			printf(" Matchin Keys %ld=%ld Payload R %ld, Payload S %ld\n", curr->matches[index+2], curr->matches[index+2], curr->matches[index],curr->matches[index+1]);
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

result* copy_results(result** resultList_arr,result* result_list){
	result* curr_head;
	result* curr_res=result_list;
	int ch_index,index,count;

	for(int i=0;i<m;i++){
		curr_head=resultList_arr[i];		

		while(curr_head!=NULL){
			ch_index=0;
			for(int j=0;j<curr_head->count;j++){
			//cout<<"eimai sto "<<j<<"-osto match tou "<<i<<"-ostou result list"<<endl;
				if( curr_res==NULL){//an exei dothei keni lista tote dimiourgw kainourgia lista apotelesmatwn
					curr_res=(result*)malloc(sizeof(result));
					if (curr_res == NULL) { fprintf(stderr, "Malloc failed \n"); return NULL;}
					curr_res->matches=(uint64_t*)malloc(size*3* sizeof(uint64_t));
					if (curr_res->matches== NULL) { fprintf(stderr, "Malloc failed \n"); return NULL;}
					
					curr_res->next=NULL;
					curr_res->matches[0]=curr_head->matches[0]; //kai arxikopoiw thn lista apotelesmatwn me ta dothenta tuples
					curr_res->matches[1]=curr_head->matches[1];
					curr_res->matches[2]=curr_head->matches[2];
					curr_res->count=1; //auksanw ton arithmo eggrafwn tis listas kata 1
					// krataw thn thesi pou exei ftasei h lista apotelesmatwn gia na th xrhsimopoihsw argotera
					result_list=curr_res;
				}
				else{//ean den einai kenh, prosthetw to apotelesma (sindiasmo tuples )sth lista

					if(curr_res->count<size){ //an to apotelesma xwraei sto bucket pou vriskomai, to kataxwrw ekei
							//printf("woo hoo\n");
						
						count=curr_res->count;
						index=count*3;
						ch_index=3*j;
						curr_res->matches[index]=curr_head->matches[ch_index]; //kai arxikopoiw thn lista apotelesmatwn me ta dothenta tuples
						curr_res->matches[index+1]=curr_head->matches[ch_index+1];
						
						curr_res->matches[index+2]=curr_head->matches[ch_index+2];
						curr_res->count+=1;

					}
					else if(curr_res->count==size){// an h lista den exei xwro, desmevw xwro enos bucket akomh sth lista kai to arxikopoiw me to dothen tup
						 
						curr_res->next=(result*)malloc(sizeof(result));
						if (curr_res->next == NULL) { fprintf(stderr, "Malloc failed \n"); return NULL;}
						curr_res->next->matches=(uint64_t*)malloc(size*3* sizeof(uint64_t));
						if (curr_res->next->matches== NULL) { fprintf(stderr, "Malloc failed \n"); return NULL;}
						
						
						ch_index=3*j;
						curr_res->next->matches[0]=curr_head->matches[ch_index]; //kai arxikopoiw thn lista apotelesmatwn me ta dothenta tuples
						curr_res->next->matches[1]=curr_head->matches[ch_index+1];
						curr_res->next->matches[2]=curr_head->matches[ch_index+2];
						curr_res->next->count=1; //auksanw kata ena to count tou neou bucket
						curr_res->next->next=NULL;
						
						curr_res=curr_res->next;

					}
					else{fprintf(stderr, "Count is wrong \n"); return NULL;}
				}

			}
			curr_head=curr_head->next;
			

		}
	}
	return result_list;
}

uint64_t** resToRowIds(result* result_list, int* numOfr){
	int  index,i;
	int numOfrows=0;
	//desmevw mnimi gia ton diasdiastato 
	uint64_t** rowIds=(uint64_t**)malloc(2*sizeof(uint64_t*));
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
	rowIds[0]=(uint64_t*)malloc(numOfrows*sizeof(uint64_t));
	if (rowIds[0]== NULL) { 
		fprintf(stderr, "Malloc failed \n"); 
		exit(-1);
	}
	rowIds[1]=(uint64_t*)malloc(numOfrows*sizeof(uint64_t));
	if (rowIds[1]== NULL) { 
		fprintf(stderr, "Malloc failed \n"); 
		exit(-1);
	}

	curr=result_list;
	int j=0;
	while(curr!=NULL){
		//printf("%d count\n",curr->count );
		for(i=0; i<curr->count; i++){
			index=i*3;

			rowIds[0][j]=curr->matches[index];
			rowIds[1][j]=curr->matches[index+1];
			
			j++;
		}
		
		curr=curr->next;

	}	
	*numOfr=numOfrows;
	//printf("numOfrows %d\n",numOfrows );
	return rowIds;
}

void free_rowIds(uint64_t** rowIds){
	 if(rowIds!=NULL){
     	free(rowIds[0]);
  
      free(rowIds[1]);
     
      free(rowIds);
    }
    rowIds=NULL;

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

	//-------------Hist---------------------------
	hist_node* R_hist=new hist_node[m];		
 	hist_node* S_hist=new hist_node [m];
 	int thr_size;
 	int start;

	//----------------New---------------------------
	relation* R_new=(relation*)malloc(sizeof(relation));
	if (R_new == NULL) { fprintf(stderr, "Malloc failed \n"); return NULL;}
	R_new->num_tuples=R->num_tuples;
	R_new->tuples=(tup*)malloc(R_new->num_tuples* sizeof(tup));
	if(R_new->tuples==NULL){ fprintf(stderr, "Malloc failed \n"); return NULL;}

	relation* S_new=(relation*)malloc(sizeof(relation));
	if (S_new == NULL) { fprintf(stderr, "Malloc failed \n"); return NULL;}
	S_new->num_tuples=S->num_tuples;
	S_new->tuples=(tup*)malloc(S_new->num_tuples* sizeof(tup));
	if(S_new->tuples==NULL){ fprintf(stderr, "Malloc failed \n"); return NULL;}

	//------------------Psum----------------------------------
	psum_node* phead=(psum_node*)malloc(m* sizeof(psum_node));
	if (phead == NULL) { fprintf(stderr, "Malloc failed \n"); return NULL;}
	psum_node* S_phead=(psum_node*)malloc(m* sizeof(psum_node));
	if (S_phead == NULL) { fprintf(stderr, "Malloc failed \n"); return NULL;}
	

	//----------------------------------------------
	hist_node** hist_list=new hist_node* [NUM_THREADS];
	
	//----------Scheduler-----------------------------------
	arguments* args=new arguments[NUM_THREADS];
	JobScheduler sch= JobScheduler(NUM_THREADS);
	JobScheduler *jsch=&sch;
	Job* job;
	
	HistJob* hist_job;
	hist_job=new HistJob[NUM_THREADS];
	
	thr_size=(R->num_tuples)/NUM_THREADS;
	start=0;
	
	for(i=0; i<NUM_THREADS; i++){
		args[i].hist_list=hist_list;
		args[i].rel=R;
		args[i].start=start;
		if(i==NUM_THREADS-1){args[i].end=R->num_tuples;}
		else{args[i].end=start+thr_size;}
		hist_job[i]=HistJob();
		hist_job[i].set_args(i, &args[i]);
		job=&hist_job[i];
		jsch->pushJob(job);
		start+=thr_size;
		//cout<<"Push i: "<<i<<endl;
	}
	jsch->waitJobs(jsch, NUM_THREADS);


	//-----Link histograms--------------------------
	for( i =0; i<m; i++){
		R_hist[i].count=0;
		for(int j=0; j<NUM_THREADS; j++){
			R_hist[i].count+=hist_list[j][i].count;
		}
	}
	
	jsch->set_ready();
	for(i=0; i<NUM_THREADS; i++){free(hist_list[i]);}	
		
	thr_size=(S->num_tuples)/NUM_THREADS;
	start=0;
	for(i=0; i<NUM_THREADS; i++){
		args[i].rel=S;
		args[i].start=start;
		if(i==NUM_THREADS-1){args[i].end=S->num_tuples;}
		else{args[i].end=start+thr_size;}
		hist_job[i]=HistJob();
		hist_job[i].set_args(i, &args[i]);
		job=&hist_job[i];
		jsch->pushJob(job);
		start+=thr_size;
	}
	
	jsch->waitJobs(jsch, NUM_THREADS);
	
	for( i =0; i<m; i++){
		S_hist[i].count=0;
		for(int j=0; j<NUM_THREADS; j++){
			S_hist[i].count+=hist_list[j][i].count;
		}
	}
	

	jsch->set_ready();	
	for(i=0; i<NUM_THREADS; i++){free(hist_list[i]);}

	
	phead=update_psumlist(phead,R_hist);
	S_phead=update_psumlist(S_phead,S_hist);


	arguments argsR[NUM_THREADS];
	PartitionJob* arrR=new PartitionJob[NUM_THREADS];
	
	//Ftixnw ta partition Jobs gia to R
	for(i=0;i<NUM_THREADS;i++){
		argsR[i].head=phead;
		argsR[i].R=R;
		argsR[i].R_new=R_new;
		argsR[i].hash_n=n;
		if(i==0){
			argsR[i].start=0;
			argsR[i].end=m/NUM_THREADS;
		}
		else if(i==NUM_THREADS-1){
			argsR[i].start=argsR[i-1].end+1;
			argsR[i].end=m-1;
		}
		else{
			argsR[i].start=argsR[i-1].end+1;
			argsR[i].end=argsR[i].start+m/NUM_THREADS;

		}
		arrR[i]=PartitionJob();
		arrR[i].set_args(i,&argsR[i]);
	}

	for(i=0;i<NUM_THREADS;i++){
		job=&arrR[i];
		jsch->pushJob(job);
	}

	arguments argsS[NUM_THREADS];
	PartitionJob* arrS=new PartitionJob[NUM_THREADS];
	
	//Ftixnw ta partition Jobs gia to S
	for(i=0;i<NUM_THREADS;i++){
		argsS[i].head=S_phead;
		argsS[i].R=S;
		argsS[i].R_new=S_new;
		argsS[i].hash_n=n;
		if(i==0){
			argsS[i].start=0;
			argsS[i].end=m/NUM_THREADS;
		}
		else if(i==NUM_THREADS-1){
			argsS[i].start=argsS[i-1].end+1;
			argsS[i].end=m-1;
		}
		else{
			argsS[i].start=argsS[i-1].end+1;
			argsS[i].end=argsS[i].start+m/NUM_THREADS;

		}
		arrS[i]=PartitionJob();
		arrS[i].set_args(i,&argsS[i]);
	}

	for(i=0;i<NUM_THREADS;i++){
		job=&arrS[i];
		jsch->pushJob(job);
	}


	jsch->waitJobs(jsch,NUM_THREADS*2);
	jsch->set_ready();	

	
	//Twra tha dimiourgisw ta join jobs
 	//gia kathe node apo tous pinakes hist twn relations

 	arguments joinArgs[m];
 	JoinJob* arrJ=new JoinJob[m];

 	result** resultList_arr=new result* [m];
 	int legit_jobs=0;

	for(i=0; i<m; i++){
		resultList_arr[i]=NULL;
		if(R_hist[i].count==0 || S_hist[i].count==0)continue;

		joinArgs[i].index=buck_compare( R_hist[i],S_hist[i]);
		joinArgs[i].list_head=&resultList_arr[i];
		joinArgs[i].curr_res=NULL;
		joinArgs[i].curr_R=&(R_hist[i]);
		joinArgs[i].curr_S=&(S_hist[i]);
		joinArgs[i].curr_Rp=&(phead[i]);
		joinArgs[i].curr_Sp=&(S_phead[i]);
		joinArgs[i].R_new=R_new;
		joinArgs[i].S_new=S_new;		


		arrJ[i]=JoinJob();
		arrJ[i].set_args(i,&(joinArgs[i]));		 
		job=&arrJ[i];		
		jsch->pushJob(job);
		legit_jobs++;

	}

	
	jsch->waitJobs(jsch,legit_jobs);
	jsch->finishJobs(jsch);	

	
	//link ta apotelesmata se mia sinoliki lista
	result_list=copy_results(resultList_arr,result_list);

	for(i=0;i<m;i++){
		free_result_list(resultList_arr[i]);
	}

	delete [] resultList_arr;

 
	//free(S_head);
	delete[] args;
	delete[] arrR;
	delete[] arrS;
	delete[] arrJ;
 	delete[] hist_list;	
	delete[] hist_job;
	delete[] R_hist;
	delete[] S_hist;

	//delete[] resultList_arr;
 	free(phead);
 	free(S_phead);
 	free(R_new->tuples);
    
    free(S_new->tuples);
	free(R_new);
	free(S_new);

 	return result_list;


}
