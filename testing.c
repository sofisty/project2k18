#include <assert.h>
#include "files.h"
#include "hash1.h"
#include "hash2.h"

#define n 8

int main(int argc,char** argv){
	
	srand ( time(NULL) ); //arxikopoiw thn rand
	tuple* rel_tR;
	tuple* rel_tS;
	char buff[500],*R_data,*S_data; //o buff einai gia thn fgets gia thn katametrhsh eggrafwn arxeiou kai mono
	int lines,num_results;
	FILE* fp=NULL;

	if(argc>3){
		printf("Wrong number of arguments.Exiting program.\n");
		exit(-1);
	}
	printf("#########################################################\n");
	printf("#############TEST CASE no #1 : All keys=1 ###############\n");
	printf("#########################################################\n");

	fp=generate_file1(fp,&lines,"Rdata.txt");
	fp=fopen("Rdata.txt","r");
	if(fp==NULL){
	    fprintf(stderr, "Failed to open file\n");
	    fflush(stderr);
	    exit(1);
	}

	rel_tR=malloc(lines*sizeof(tuple));

	store_file(fp,buff,500,rel_tR,lines); //apothikevw to arxeio se morfh tuples eggrafwn gia to relation R
	fclose(fp);

	relation* R=malloc(sizeof(relation));
	if (R == NULL) { fprintf(stderr, "Malloc failed \n"); return 1;}
	R->tuples=rel_tR;
	R->num_tuples=lines;


 	//AKOLOUTHEI AKRIVWS H IDIA DIADIKASIA GIA TO S
 	//printf("-----Now for relation S\n"); 
 	

 	fp=generate_file1(fp,&lines,"Sdata.txt");
	fp=fopen("Sdata.txt","r");
	if(fp==NULL){
	    fprintf(stderr, "Failed to open file\n");
	    fflush(stderr);
	    exit(1);
	}

	rel_tS=malloc(lines*sizeof(tuple));
	store_file(fp,buff,200,rel_tS,lines);
	fclose(fp);

	relation* S=malloc(sizeof(relation));
	if (S == NULL) { fprintf(stderr, "Malloc failed \n"); return 1;}
	S->tuples=rel_tS;
	S->num_tuples=lines; 

//----------------------------------------------------------------------------------------
//----------------------------------------------------------------------------------------
	result* result_list=NULL;
	result_list=RadixHashJoin(R, S);

	print_results( result_list,&num_results);
	assert(num_results==(R->num_tuples*S->num_tuples)); //ean den isxiei h mesa sinthikh to programma termatizei me error
	free_result_list( result_list);	

	//Apodesmevw ti mnimi pou eixa desmefsei gia oles tis domes kai metavlites 

	free(rel_tR);
	free(rel_tS);
	free(R);
	free(S);

	printf("#########################################################\n");
	printf("###########TEST CASE no #2 : R odd and S even ###########\n");
	printf("#########################################################\n");

	fp=generate_file2(fp,&lines,"Rdata.txt");
	fp=fopen("Rdata.txt","r");
	if(fp==NULL){
	    fprintf(stderr, "Failed to open file\n");
	    fflush(stderr);
	    exit(1);
	}

	rel_tR=malloc(lines*sizeof(tuple));

	store_file(fp,buff,500,rel_tR,lines); //apothikevw to arxeio se morfh tuples eggrafwn gia to relation R
	fclose(fp);

	R=malloc(sizeof(relation));
	if (R == NULL) { fprintf(stderr, "Malloc failed \n"); return 1;}
	R->tuples=rel_tR;
	R->num_tuples=lines;


 	//AKOLOUTHEI AKRIVWS H IDIA DIADIKASIA GIA TO S
 	//printf("-----Now for relation S\n"); 
 	

 	fp=generate_file3(fp,&lines,"Sdata.txt");
	fp=fopen("Sdata.txt","r");
	if(fp==NULL){
	    fprintf(stderr, "Failed to open file\n");
	    fflush(stderr);
	    exit(1);
	}

	rel_tS=malloc(lines*sizeof(tuple));
	store_file(fp,buff,200,rel_tS,lines);
	fclose(fp);

	S=malloc(sizeof(relation));
	if (S == NULL) { fprintf(stderr, "Malloc failed \n"); return 1;}
	S->tuples=rel_tS;
	S->num_tuples=lines; 

//----------------------------------------------------------------------------------------
//----------------------------------------------------------------------------------------
	result_list=NULL;
	result_list=RadixHashJoin(R, S);

	print_results( result_list,&num_results);
	assert(num_results==0); //ean den isxiei h mesa sinthikh to programma termatizei me error
	free_result_list( result_list);	
 	
//---------------------------------------------------------------------------------------

	
//Apodesmevw ti mnimi pou eixa desmefsei gia oles tis domes kai metavlites 

	free(rel_tR);
	free(rel_tS);
	free(R);
	free(S);
 

	return 0;
}

