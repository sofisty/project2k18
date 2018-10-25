#include "files.h"
#include "hash1.h"
#include "hash2.h"

int main(int argc,char** argv){
	
	//int  values[10] = { 0b100100, 0b111001, 0b100001, 0b101101, 0b110100, 0b101101, 0b101001, 0b100101, 0b001101, 0b111100};
	//int  values[10] = { 0b100100, 0b100100, 0b100100,0b100100, 0b100100, 0b100100, 0b100100,0b100100, 0b100100, 0b100100};
	//int values[15]={0b10001,0b11011,0b10111,0b11101,0b11100,0b10011,0b10000,0b101111, 0b10100, 0b10100,0b110101,0b101001,0b100011, 0b100110, 0b10100 };
	//int values[13]={00000,00000,00000,00000,00000,00000,00000,00000,00000,00000,00000,00000,00000};
	srand ( time(NULL) );
	int i, total_bucketsR=0, total_bucketsS=0;

	tuple* rel_tR;
	char buff[500],*R_data,*S_data;
	int lines;
	FILE* fp=NULL;

	if(argc>3){
		printf("Wrong number of arguments.Exiting program.\n");
		exit(-1);
	}
	if(argc==1){
		fp=generate_file(fp,&lines,"Rdata.txt");
		fp=fopen("Rdata.txt","r");
		if(fp==NULL){
		    fprintf(stderr, "Failed to open file\n");
		    fflush(stderr);
		    exit(1);
		}
	}
	if((argc==3) || (argc==2)){ //an dinetai ena file name apo to command line to R na dimiourgeitai apo auto 
		R_data=malloc((strlen(argv[1])+1)*sizeof(char));
		R_data=strcpy(R_data, argv[1]);

		fp= fopen(R_data, "r"); //Open and read binary file binfile

		if(fp==NULL){
		    fprintf(stderr, "Failed to open file\n");
		    fflush(stderr);
		    exit(1);
		}
		lines=count_lines(fp);
		rewind(fp);
	}
	
	rel_tR=malloc(lines*sizeof(tuple));

	store_file(fp,buff,200,rel_tR,lines);
	fclose(fp);

	relation* R=malloc(sizeof(relation));
	if (R == NULL) { fprintf(stderr, "Malloc failed \n"); return 1;}

	relation* R_new=malloc(sizeof(relation));
	if (R_new == NULL) { fprintf(stderr, "Malloc failed \n"); return 1;}


	R->tuples=rel_tR;
	R->num_tuples=lines; //allakse to 


	int n=4, hash_val;
	hist_node * R_head = NULL;
	psum_node* phead=NULL;

	phead=NULL;

	for(i =0; i<R->num_tuples; i++){
		hash_val= hash_func( (R->tuples[i].key), n);
		printf("%d, %d\n", R->tuples[i].key, hash_val);
		R_head=update_hist(R_head, hash_val, &total_bucketsR);

	}
	printf("------TOTAL BUCKETS %d\n",total_bucketsR );

	//print_hist( R_head);

	phead=create_psumlist( phead, R_head) ;
	//printf("\n\n");
 	//print_psum(phead);

 	R_new=reorder_R(phead,  R,  R_new,  n  );
 	print_R( R);

 	//print_R(R_new);
 	

 	//-----------------------------------------------------
 	printf("-----Now for relation S\n");
 	//int values2[10]={0b111100, 0b111001, 0b100111, 0b101100, 0b100000, 0b111111, 0b101111, 0b000011, 0b111110, 0b110000};
 	

 	//int values2[17]={0b10100, 0b10111, 0b10100, 0b10001, 0b10000, 0b10100, 0b10111, 0b11001,  0b10000, 0b101011, 0b100110, 0b10101, 0b10011, 0b10000, 0b010101, 0b1001101, 0b10000};
 	//int values2[15]={00000,00000,00000,00000,00000,00000,00000,00000,00000,00000,00000,00000,00000,00000,00000};
 	tuple* rel_tS;

 	if((argc==1) || (argc==2)){ //an exei dothei ena orisma dimiourgei to relation R, opote to S ginetai pali generate
		fp=generate_file(fp,&lines,"Sdata.txt");
		fp=fopen("Sdata.txt","r");
		if(fp==NULL){
		    fprintf(stderr, "Failed to open file\n");
		    fflush(stderr);
		    exit(1);
		}
	}
	if(argc==3){
		S_data=malloc((strlen(argv[2])+1)*sizeof(char));
		S_data=strcpy(S_data, argv[2]);

		fp= fopen(S_data, "r"); //Open and read binary file binfile

		if(fp==NULL){
		    fprintf(stderr, "Failed to open file\n");
		    fflush(stderr);
		    exit(1);
		}
		lines=count_lines(fp);
		rewind(fp);
	}

	rel_tS=malloc(lines*sizeof(tuple));
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
	printf("------TOTAL BUCKETS %d\n",total_bucketsS );

	//print_hist( S_head);

	S_phead=create_psumlist( S_phead, S_head) ;
	//printf("\n\n");
 	//print_psum(phead);

 	S_new=reorder_R(S_phead,  S,  S_new,  n  );
 	//print_R( S);

 	print_R(S_new);

 	//print_R( S);
 	//printf("\n\n");
 	//print_R(S_new);
 	//--------------------------------------------------------------------------------------

 	final_hash(R_head, S_head, phead, S_phead, R_new, S_new);

	//----------------------------------------------------------------------------------
 	if(argc==3){
 		free(R_data);
		free(S_data);
 	}
 	if(argc==2){
 		free(R_data);
 	}
	

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

}
