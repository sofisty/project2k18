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

	//9 1 11|0.2=1.0&1.0=2.1&1.0=0.2&0.3>3991|1.0  (20)
	//interm_node* filter(interm_node* interm,int oper, infoNode* infoMap, int rel, int indexOfrel, int col, uint64_t value, int numOfrels);
	/*
	interm=filter( interm,1, infoMap, 9, 0, 3, 3991, 14);
	//interm_node* join2(interm_node* interm, infoNode* infoMap, joinHistory** joinHist,int rel1,int indexOfrel1,int rel2,int indexOfrel2, int col1, int col2, int numOfrels);
	interm= join2( interm, infoMap, &joinHist,1,1,11,2, 0, 1, 14);
	//statusOfinterm( interm);

	interm= join2( interm, infoMap, &joinHist,9,0,1,1, 2, 0, 14);
	//statusOfinterm( interm);
	
	
	interm= join2( interm, infoMap, &joinHist,1,1,9,0, 0, 2, 14);
	//statusOfinterm( interm);

	uint64_t sum1=0;
	for(int i=0; i<	interm->numOfrows[1]; i++){
		sum1+= return_value( infoMap, 1 ,0, interm->rowIds[1][i]);

	}

	printf("SUM1 %ld\n",sum1 );

	*/
	//13 0 2|0.2=1.0&1.0=0.1&1.0=2.2&0.1>4477|2.0 2.3 1.2 (proteleutaio)
	/*interm=filter( interm,1, infoMap, 13, 0, 1, 4477, 14);
	
	interm= join2( interm, infoMap, &joinHist,13,0,0,1, 2, 0, 14);
	interm= join2( interm, infoMap, &joinHist,0,1,13,0, 0, 1, 14);
	interm= join2( interm, infoMap, &joinHist,0,1,2,2, 0, 2, 14);
	statusOfinterm(interm);
	uint64_t sum1=0, sum2=0, sum3=0;
	for(int i=0; i<	interm->numOfrows[2]; i++){
		sum1+= return_value( infoMap, 2 ,0, interm->rowIds[2][i]);

	}
	for(int i=0; i<	interm->numOfrows[2]; i++){
		sum2+= return_value( infoMap, 2 ,3, interm->rowIds[2][i]);

	}

	for(int i=0; i<	interm->numOfrows[1]; i++){
		sum3+= return_value( infoMap, 0 ,2, interm->rowIds[1][i]);

	}

	printf("SUM1 %ld SUM2 %ld SUM3 %ld\n",sum1, sum2, sum3 );
	

*/

	//9 1 12|0.2=1.0&1.0=2.1&2.2=1.0&0.2<2685|2.0 (23)
	//1 12 2|0.0=1.2&0.0=2.1&1.1=0.0&1.0>25064|0.2 1.3 (24)
	//7 0 9|0.1=1.0&1.0=0.1&1.0=2.1&0.1>3791|1.2 1.2 (35)
	//7 1 3|0.2=1.0&1.0=2.1&1.0=0.2&0.2>6082|2.3 2.1 (38)
	//13 0 2|0.2=1.0&1.0=0.1&1.0=2.2&0.1>4477|2.0 2.3 1.2 (proteleutaio)
	//interm_node* filter(interm_node* interm,int oper, infoNode* infoMap, int rel, int indexOfrel, int col, uint64_t value, int numOfrels);
	return 0;
}
