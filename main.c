#include "files.h"
#include "hash1.h"
#include "hash2.h"
#include "testing.h"

#include "testfiles.h"
#include"query.h"

#define n 8

int main(int argc,char** argv){
	int i =0, numOffiles=0, num_batches;
	char file[250], buff[400];
	batch* b=NULL;
	long int offset=0, prev_offset=0;
  
	RelFiles* relList=NULL;
	RelFiles* relCurr=relList;
	infoNode* infoMap=NULL;

	while(1){
		printf("Enter file: \n");


		if(	scanf("%s",file)==-1) break;
		numOffiles+=1;

		
	relCurr=add_Relation(&relList, relCurr, file);

	}
	print_RelFiles( relList);
	printf("total files %d\n",numOffiles );

	infoMap=create_InfoMap(relList,  infoMap, numOffiles);
	print_InfoMap( infoMap, numOffiles);

 

//----------------------------------------------------------------------------------------
//----------------------------------------------------------------------------------------
	/*result* result_list=NULL;
	result_list=RadixHashJoin(R, S);

	print_results( result_list,&num_results);
	free_result_list( result_list);	
	*/
 	
//---------------------------------------------------------------------------------------


	FILE* fp=fopen("small.work","r"); //anoigei to arxeio eperwthsewn
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



	interm_node* interm=NULL;


	interm=filter(interm,'>', infoMap, 1, 1, 0, 9477,3);
	//dinei 650 apotel
	
	interm=filter(interm,'>', infoMap, 0,0, 1, 100000000000,3);
	// 0 apot

	interm=filter(interm,'>', infoMap, 0,0, 1, 100000000000,3);
	//ksana 0

	interm=filter(interm, '=', infoMap, 1,1,0, 9501,3);
	//dinei 1

	interm=filter(interm,'>', infoMap, 0,0, 0, 1000,3);
	//pali 0

	interm=filter(interm, '<', infoMap,2, 2,1, 1000,3);
	//dinei 2292
	
	for(int i=0; i<3; i++){
		printf("rel %d : numOfrows: %d\n",i, interm->numOfrows[i] );
		
	}
	return 0;
}
