int main(int argc,char** argv){

	if(argc!=3){
		printf("Wrong number of arguments.Exiting program.\n");
		exit(-1);
	}
	char* R_data=malloc((strlen(argv[1])+1)*sizeof(char));
	R_data=strcpy(R_data, argv[1]);

	char* S_data=malloc((strlen(argv[2])+1)*sizeof(char));
	S_data=strcpy(S_data, argv[2]);

	
	int i, total_bucketsR=0, total_bucketsS=0, hash_size, bucket_size;

	tuple* rel_tR;
	char buff[200];


	FILE *fp= fopen(R_data, "r"); //Open and read binary file binfile
	if(fp==NULL){
	    fprintf(stderr, "Failed to open file\n");
	    fflush(stderr);
	    exit(1);
	}

	int lines=count_lines(fp);
	//printf("--------------%d\n\n",lines);
	rel_tR=malloc(lines*sizeof(tuple));

	rewind(fp);
	store_file(fp,buff,200,rel_tR,lines);




	fclose(fp);


	relation* R=malloc(sizeof(relation));
	if (R == NULL) { fprintf(stderr, "Malloc failed \n"); return 1;}

	relation* R_new=malloc(sizeof(relation));
	if (R_new == NULL) { fprintf(stderr, "Malloc failed \n"); return 1;}

	
	R->tuples=rel_tR;
	R->num_tuples=lines; //allakse to 

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

 	R_new=reorder_R(phead,  R,  R_new,  n  );

 	print_R(R_new);
 	

 	//-----------------------------------------------------
 	printf("-----Now for relation S\n");
 	
	tuple* rel_tS;


	fp= fopen(S_data, "r"); //Open and read binary file binfile
	if(fp==NULL){
	    fprintf(stderr, "Failed to open file\n");
	    fflush(stderr);
	    exit(1);
	}

	lines=count_lines(fp);
	//printf("--------------%d\n\n",lines);
	rel_tS=malloc(lines*sizeof(tuple));

	rewind(fp);
	store_file(fp,buff,200,rel_tS,lines);




	fclose(fp);

	relation* S=malloc(sizeof(relation));
	if (S == NULL) { fprintf(stderr, "Malloc failed \n"); return 1;}

	relation* S_new=malloc(sizeof(relation));
	if (S_new == NULL) { fprintf(stderr, "Malloc failed \n"); return 1;}


	S->tuples=rel_tS;
	S->num_tuples=lines; //allakse to 

	hist_node* S_head = NULL;
	psum_node* S_phead=NULL;

	for(i =0; i<S->num_tuples; i++){
		hash_val= hash_func( (S->tuples[i].key), n);
		printf("%d, %d\n", S->tuples[i].key, hash_val);
		S_head=update_hist(S_head, hash_val, &total_bucketsS);

	}
	print_hist( S_head);

	S_phead=create_psumlist( S_phead, S_head) ;
	
 	//kanw reorder kai to S
 	S_new=reorder_R(S_phead,  S,  S_new,  n  );

 	print_R(S_new);
 	//--------------------------------------------------------------------------------------

	//AUTHAIRETH DOKIMH STO PRWTO BUCKET TOU R
 	/*final_hash(R_head, S_head, phead, S_phead);
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
	*/

	//----------------------------------------------------------------------------------

	//free_bucket(hash_size, bucket); //gia kathe bucket kai chain pou ftiaxnetai 
	//free_chain(bucket_size, chain); 
	
	//free_result_list( result_list);
	
	free(R_data);
	free(S_data);

	free_hist(R_head);
	free_hist(S_head);
 	free_psum(phead);
 	free_psum(S_phead);
 	free(rel_tR);
 	free(rel_tS);
 	free(R);
 	free(S);
 	free(R_new->tuples);
    free(S_new->tuples);
	free(R_new);
	free(S_new);


	//free(bucket);
	//free(chain);

	


}