#include "files.h"
#include "hash1.h"
#include "hash2.h"
#include "testing.h"

#include "testfiles.h"
#include"query.h"

#define n 8

int main(int argc,char** argv){
	/*int i =0, numOffiles=0;
	char file[250];
	int* rowIds;
 
  
	RelFiles* relList=NULL;
	RelFiles* relCurr=relList;
	infoNode* infoMap=NULL;

	while(1){
		printf("Enter file: \n");


		if(	scanf("%s",file)==-1) break;
		numOffiles+=1;

		printf("%s\n",file );
	relCurr=add_Relation(&relList, relCurr, file);


	}
	print_RelFiles( relList);
	printf("total files %d\n",numOffiles );

	infoMap=create_InfoMap(relList,  infoMap, numOffiles);
	print_InfoMap( infoMap, numOffiles);

	rowIds=filter('>', infoMap, 0, 0, 4635);

	i=0;
	while(rowIds[i]!=-1){printf("%d\n",rowIds[i] ); i++;}
	

	srand ( time(NULL) ); //arxikopoiw thn rand
	tuple* rel_tR;
	tuple* rel_tS;
	char buff[500],answer,*R_data,*S_data; //o buff einai gia thn fgets gia thn katametrhsh eggrafwn arxeiou kai mono
	int lines,num_results;
	FILE* fp=NULL;

	if(argc>3){
		printf("Wrong number of arguments.Exiting program.\n");
		exit(-1);
	}

	printf("Would you like to run some tests before running the program? Y/N\n");
	scanf("  %c", &answer);
	if((answer=='Y')||(answer=='y')){
		printf("Running some tests.Please wait...\n");
		testing();
		printf("Testing is completed.Check the directory for the result files.\n\n");
	} 

	if(argc==1){ //den mou dinontai arxeia gia na xrisimopoihsw opote dimiourgw eksarxhs ta arxeia pou tha xtisoun ta R kai S
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
		rewind(fp); //meta th katametrhsh eggrafwn tou arxeiou epistrefw ton fp sthn arxh tou arxeiou
	}
	
	rel_tR=malloc(lines*sizeof(tuple));

	store_file(fp,buff,200,rel_tR,lines); //apothikevw to arxeio se morfh tuples eggrafwn gia to relation R
	fclose(fp);

	relation* R=malloc(sizeof(relation));
	if (R == NULL) { fprintf(stderr, "Malloc failed \n"); return 1;}
	R->tuples=rel_tR;
	R->num_tuples=lines;



 	//AKOLOUTHEI AKRIVWS H IDIA DIADIKASIA GIA TO S
 	//printf("-----Now for relation S\n"); 
 	

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
	S->tuples=rel_tS;
	S->num_tuples=lines; 


//----------------------------------------------------------------------------------------
//----------------------------------------------------------------------------------------
	result* result_list=NULL;
	result_list=RadixHashJoin(R, S);

	print_results( result_list,&num_results);
	free_result_list( result_list);	
 	
//---------------------------------------------------------------------------------------

 	
	//Apodesmevw ti mnimi pou eixa desmefsei gia oles tis domes kai metavlites 
 	if(argc==3){
 		free(R_data);
		free(S_data);
 	}
 	if(argc==2){
 		free(R_data);
 	}
	
	
 	free(rel_tR);
 	free(rel_tS);
 	free(R);
 	free(S);
 
*/

	char buff[400];
	int num_batches,i;
	batch* b=NULL;
	long int offset=0, prev_offset=0;

	FILE* fp=fopen("sm.work","r"); //anoigei to arxeio eperwthsewn
	num_batches=count_batches(fp); //kai metraei posa batches eperwthsewn exei
	printf("This file has %d batches of queries.\n",num_batches );
	rewind(fp);
	i=0;
	for(i=0;i<num_batches;i++){
		b=store_batch(fp,&offset,&prev_offset,buff,400,b);
		//print_batch(b);		
		prev_offset=offset;
		//free_batch(b);
	}

	fclose(fp);

	return 0;
}
