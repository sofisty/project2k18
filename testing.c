#include <assert.h>
#include "files.h"
#include "hash1.h"
#include "hash2.h"

#define n 8

FILE* store_test_res(result* result_list, FILE* fp, char* filename, int* resfortest){ //apothikevw th lista apotelesmatwn
	int count, i,index, b=0, total=0;
	result* curr=result_list;

	fp=fopen(filename,"w"); //graffei sto arxeio eggrafes ths morfhs [row_id    value]
	fprintf(fp,"--RESULTS--\n");
	if(result_list==NULL) fprintf(fp,"0 results found!\n");
	while(curr!=NULL){
		//fprintf(fp,"\n-Bucket: %d\n", b);
		count=curr->count;
		for(i=0; i<count; i++){
			total++;
			index=i*3;
			//fprintf(fp," Matchin Keys %d=%d Payload R %d, Payload S %d\n", curr->matches[index+2], curr->matches[index+2], curr->matches[index],curr->matches[index+1]);
		}
		curr=curr->next;
		b++;
	}
	*resfortest=total;
	fprintf(fp,"~~~Total results %d ~~~~~\n",total );
	fclose(fp);
	return fp;
}

FILE* testing(void){
	
	srand ( time(NULL) ); //arxikopoiw thn rand
	tuple* rel_tR;
	tuple* rel_tS;
	char buff[500]; //o buff einai gia thn fgets gia thn katametrhsh eggrafwn arxeiou kai mono
	int lines,num_results;
	FILE* fp=NULL;

	
	fp=generate_file1(fp,&lines,"Rtest1.txt");
	fp=fopen("Rtest1.txt","r");
	if(fp==NULL){
	    fprintf(stderr, "Failed to open file\n");
	    fflush(stderr);
	    exit(1);
	}

	rel_tR=malloc(lines*sizeof(tuple));

	store_file(fp,buff,500,rel_tR,lines); //apothikevw to arxeio se morfh tuples eggrafwn gia to relation R
	fclose(fp);

	relation* R=malloc(sizeof(relation));
	if (R == NULL) { fprintf(stderr, "Malloc failed \n"); return NULL;}
	R->tuples=rel_tR;
	R->num_tuples=lines;


 	//AKOLOUTHEI AKRIVWS H IDIA DIADIKASIA GIA TO S
 	//printf("-----Now for relation S\n"); 
 	

 	fp=generate_file1(fp,&lines,"Stest1.txt");
	fp=fopen("Stest1.txt","r");
	if(fp==NULL){
	    fprintf(stderr, "Failed to open file\n");
	    fflush(stderr);
	    exit(1);
	}

	rel_tS=malloc(lines*sizeof(tuple));
	store_file(fp,buff,200,rel_tS,lines);
	fclose(fp);

	relation* S=malloc(sizeof(relation));
	if (S == NULL) { fprintf(stderr, "Malloc failed \n"); return NULL;}
	S->tuples=rel_tS;
	S->num_tuples=lines; 

//----------------------------------------------------------------------------------------
//----------------------------------------------------------------------------------------
	result* result_list=NULL;
	result_list=RadixHashJoin(R, S);

	//storing result into a new file
	fp=fopen("test1_res.txt","w+");
	fprintf(fp,"#########################################################\n");
	fprintf(fp,"#############TEST CASE no #1 : All keys=1 ###############\n");
	fprintf(fp,"#########################################################\n");
	fclose(fp);

	fp=store_test_res(result_list, fp,"test1_res.txt", &num_results);

	assert(num_results==(R->num_tuples*S->num_tuples)); //ean den isxiei h mesa sinthikh to programma termatizei me error
	
	free_result_list( result_list);	

	//Apodesmevw ti mnimi pou eixa desmefsei gia oles tis domes kai metavlites 

	free(rel_tR);
	free(rel_tS);
	free(R);
	free(S);

	//Case 2 - ODDS AND EVENS

	fp=generate_file2(fp,&lines,"Rtest2.txt");
	fp=fopen("Rtest2.txt","r");
	if(fp==NULL){
	    fprintf(stderr, "Failed to open file\n");
	    fflush(stderr);
	    exit(1);
	}

	rel_tR=malloc(lines*sizeof(tuple));

	store_file(fp,buff,500,rel_tR,lines); //apothikevw to arxeio se morfh tuples eggrafwn gia to relation R
	fclose(fp);

	R=malloc(sizeof(relation));
	if (R == NULL) { fprintf(stderr, "Malloc failed \n"); return NULL;}
	R->tuples=rel_tR;
	R->num_tuples=lines;


 	//AKOLOUTHEI AKRIVWS H IDIA DIADIKASIA GIA TO S
 	//printf("-----Now for relation S\n"); 
 	

 	fp=generate_file1(fp,&lines,"Stest2.txt");
	fp=fopen("Stest2.txt","r");
	if(fp==NULL){
	    fprintf(stderr, "Failed to open file\n");
	    fflush(stderr);
	    exit(1);
	}

	rel_tS=malloc(lines*sizeof(tuple));
	store_file(fp,buff,200,rel_tS,lines);
	fclose(fp);

	S=malloc(sizeof(relation));
	if (S == NULL) { fprintf(stderr, "Malloc failed \n"); return NULL;}
	S->tuples=rel_tS;
	S->num_tuples=lines; 

//----------------------------------------------------------------------------------------
//----------------------------------------------------------------------------------------
	result_list=NULL;
	result_list=RadixHashJoin(R, S);

	//storing result into a new file
	fp=fopen("test2_res.txt","w+");
	fprintf(fp,"#########################################################\n");
	fprintf(fp,"###########TEST CASE no #2 : R odd and S even ###########\n");
	fprintf(fp,"#########################################################\n");
	fclose(fp);

	fp=store_test_res(result_list, fp,"test2_res.txt", &num_results);

	assert(num_results==0); //ean den isxiei h mesa sinthikh to programma termatizei me error
	
	free_result_list( result_list);	

	//Apodesmevw ti mnimi pou eixa desmefsei gia oles tis domes kai metavlites 

	free(rel_tR);
	free(rel_tS);
	free(R);
	free(S);


	//Case 3 - (row, key)=(i,i)

	fp=generate_file4(fp,&lines,"Rtest3.txt");
	fp=fopen("Rtest3.txt","r");
	if(fp==NULL){
	    fprintf(stderr, "Failed to open file\n");
	    fflush(stderr);
	    exit(1);
	}

	rel_tR=malloc(lines*sizeof(tuple));

	store_file(fp,buff,500,rel_tR,lines); //apothikevw to arxeio se morfh tuples eggrafwn gia to relation R
	fclose(fp);

	R=malloc(sizeof(relation));
	if (R == NULL) { fprintf(stderr, "Malloc failed \n"); return NULL;}
	R->tuples=rel_tR;
	R->num_tuples=lines;
	

 	//AKOLOUTHEI AKRIVWS H IDIA DIADIKASIA GIA TO S
 	//printf("-----Now for relation S\n"); 
 	

 	fp=generate_file4(fp,&lines,"Stest3.txt");
	fp=fopen("Stest3.txt","r");
	if(fp==NULL){
	    fprintf(stderr, "Failed to open file\n");
	    fflush(stderr);
	    exit(1);
	}

	rel_tS=malloc(lines*sizeof(tuple));
	store_file(fp,buff,200,rel_tS,lines);
	fclose(fp);

	S=malloc(sizeof(relation));
	if (S == NULL) { fprintf(stderr, "Malloc failed \n"); return NULL;}
	S->tuples=rel_tS;
	S->num_tuples=lines; 



//----------------------------------------------------------------------------------------
//----------------------------------------------------------------------------------------
	result_list=NULL;
	result_list=RadixHashJoin(R, S);

	//storing result into a new file
	fp=fopen("test3_res.txt","w+");
	fprintf(fp,"#########################################################\n");
	fprintf(fp,"######TEST CASE no #3 : (rowId, key)=(rowId, rowId)######\n");
	fprintf(fp,"#########################################################\n");
	fclose(fp);

	fp=store_test_res(result_list, fp,"test3_res.txt", &num_results);

	if(R->num_tuples>=S->num_tuples) assert(num_results==S->num_tuples);//ean den isxiei h mesa sinthikh to programma termatizei me error
	else assert(num_results==R->num_tuples);

	free_result_list( result_list);	

	//Apodesmevw ti mnimi pou eixa desmefsei gia oles tis domes kai metavlites 

	free(rel_tR);
	free(rel_tS);
	free(R);
	free(S);
 
	return 0;
}

