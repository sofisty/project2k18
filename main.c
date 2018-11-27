#include "files.h"
#include "hash1.h"
#include "hash2.h"
#include "testing.h"


#include "testfiles.h"
#include"query.h"
#include "join.h"

#define n 8

int main(int argc,char** argv){
	int i =0, numOffiles=0, num_batches;
	char file[250], buff[400];
	batch* b=NULL;
	long int offset=0, prev_offset=0;
  	char init_file[20];
	RelFiles* relList=NULL;
	RelFiles* relCurr=relList;
	infoNode* infoMap=NULL;
 	FILE* fp;
   
	printf("Enter init file: \n");
	scanf("%s", init_file);
	fp = fopen(init_file, "r");
	if(fp==NULL){fprintf(stderr, "Cannot open init_file \n"); return 1;}
	while(fscanf(fp, "%s", file)>0){
		
		numOffiles+=1;
	
	relCurr=add_Relation(&relList, relCurr, file);

	}
	print_RelFiles( relList);
	printf("total files %d\n",numOffiles );

	infoMap=create_InfoMap(relList,  infoMap, numOffiles);
	//print_InfoMap( infoMap, numOffiles);

 

//----------------------------------------------------------------------------------------
//----------------------------------------------------------------------------------------
	/*result* result_list=NULL;
	result_list=RadixHashJoin(R, S);

	print_results( result_list,&num_results);
	free_result_list( result_list);	
	*/
 	
//---------------------------------------------------------------------------------------
/*

	FILE* fp=fopen("sm.work","r"); //anoigei to arxeio eperwthsewn
	num_batches=count_batches(fp); //kai metraei posa batches eperwthsewn exei
	printf("This file has %d batches of queries.\n",num_batches );
	rewind(fp);
	i=0;
	for(i=0;i<num_batches;i++){
		b=store_batch(fp,&offset,&prev_offset,buff,400,b);
		print_batch(b);		
		prev_offset=offset;
		free_batch(b);
	}
	fclose(fp);

*/
	interm_node* interm=NULL;
	result* result_list=NULL;
	joinHistory* joinHist=NULL;

	execute_workload("small.work",14,infoMap);
	
	return 0;
}
