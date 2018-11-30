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
  	char init_file[20], work_file[40];
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
  cross_list* list=NULL;

  //interm=filter(interm, 1, infoMap,6,6,0,2000,14);
  //interm=join2(interm, infoMap, &joinHist, 0,0,1,1, 0, 2, 14);
    //interm=join2(interm, infoMap, &joinHist, 2,2,3,3, 0, 2, 14);
    //list=cross_nodes(interm,infoMap, &joinHist, 14);
    //free_crossList(list);
    //statusOfCrossList(list);
    //statusOfinterm( interm);
  	printf("Enter workload: \n");
	scanf("%s", work_file);
  	execute_workload(work_file,14,infoMap);
	/*printf("####### join 1 #################\n");
	interm= join2( interm, infoMap, &joinHist,0,0, 1,1, 1,0, 14);
	print_joinHist(joinHist);
	 //print_joinHist( joinHist, 3);
	printf("####### join 2 #################\n");
	interm= join2( interm, infoMap, &joinHist,3,3, 2,2, 1,0, 14);
	  print_joinHist(joinHist);
	
	printf("####### join 3 #################\n");
	interm= join2( interm, infoMap, &joinHist,4,4, 5,5, 1,0, 14);
	  print_joinHist(joinHist);
	
	printf("####### join 4 #################\n");
	interm= join2( interm, infoMap, &joinHist,6,6, 7,7, 1,0, 14);
	  print_joinHist(joinHist);
	
	printf("####### join 5 #################\n");
	interm= join2( interm, infoMap, &joinHist,0,0, 7,7, 1,0, 14);
	  print_joinHist(joinHist);
	
	printf("####### join  6#################\n");
	interm= join2( interm, infoMap, &joinHist,3,3, 4,4, 1,0, 14);
	print_joinHist(joinHist);

	printf("####### join  7#################\n");
	interm= join2( interm, infoMap, &joinHist,0,0, 4,4, 1,0, 14);
	print_joinHist(joinHist);

	*/
	return 0;
}
