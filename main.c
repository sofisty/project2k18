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

	//3 0 1|0.2=1.0&0.1=2.0&0.2>3499|1.2 0.1
	interm=filter(interm,1, infoMap, 3,0, 2, 3499,4);

	printf("####### join 1 #################\n");
	interm= join2( interm, infoMap, &joinHist,3,0, 0,1, 2,0, 4);
	print_joinHist(joinHist, 4);

	/*printf("####### join 2 #################\n");
	interm= join2( interm, infoMap, &joinHist,3,0, 1,2, 1,0, 4);
	print_joinHist(joinHist, 4);
	*/

	uint64_t sum1=0 , sum2=0;
	for(int i=0; i<interm->numOfrows[0]; i++ ){
		sum1+=return_value( infoMap, 3 ,1, interm->rowIds[0][i]);
	}
	printf(" SUM1 %ld \n", sum1);

	printf("numOfrows %d \n",interm->numOfrows[1] );
	for(int i=0; i<interm->numOfrows[1]; i++ ){
		sum2+=return_value( infoMap, 0 ,2, interm->rowIds[1][i]);
	}
	printf(" SUM2 %ld \n", sum2);
	/*


	interm_node* interm=NULL;
	interm=filter(interm,1, infoMap, 1,1, 0, 9477,10);
	//dinei 650 apotel
	
	//interm=filter(interm,1, infoMap, 0, 1,1, 100000000000,2);
	// 0 apot

	//interm=filter(interm, 1, infoMap, 0, 1, 1, 100000000000,2);
	//ksana 0

	//interm=filter(interm, 3, infoMap, 1, 0, 0, 9501, 2);
	//dinei 1

	//interm=filter(interm,1, infoMap, 0, 1, 0, 1000, 2);
	//pali 0

	//interm=filter(interm, 2, infoMap, 2, 2, 1, 1000, 3);
	//dinei 2292

	//int rowIds[100], *newRowIds;
	//for(i=0; i<100; i++)rowIds[i]=i;

	//newRowIds= real_RowIds(interm, rowIds, 100, 0, newRowIds);

	/*for(i=0; i<100; i++){
		printf("%d\n", newRowIds[i]);
	}*/


	//joinHist= add_nodeHistory(1, joinHist, 3);
	//joinHist= add_nodeHistory(0, joinHist, 3);

	//rel1, ind1, rel2, ind2, col1,col2 
	
/*
	printf("####### join 1 #################\n");
	interm= join2( interm, infoMap, &joinHist,0,0, 1,1, 1,0, 10);
	print_joinHist(joinHist, 10);
	 //print_joinHist( joinHist, 3);
	printf("####### join 2 #################\n");
	interm= join2( interm, infoMap, &joinHist,3,3, 2,2, 1,0, 10);
	  print_joinHist(joinHist, 10);
	
	printf("####### join 3 #################\n");
	interm= join2( interm, infoMap, &joinHist,4,4, 5,5, 1,0, 10);
	  print_joinHist(joinHist, 10);
	
	printf("####### join 4 #################\n");
	interm= join2( interm, infoMap, &joinHist,6,6, 7,7, 1,0, 10);
	  print_joinHist(joinHist, 10);
	
	printf("####### join 5 #################\n");
	interm= join2( interm, infoMap, &joinHist,0,0, 7,7, 1,0, 10);
	  print_joinHist(joinHist, 10);
	
	printf("####### join  6#################\n");
	interm= join2( interm, infoMap, &joinHist,3,3, 4,4, 1,0, 10);
	print_joinHist(joinHist, 10);
*/
	return 0;
}
