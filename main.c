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
 

	return 0;
}