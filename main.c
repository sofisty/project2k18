#include "files.h"
#include "hash1.h"
#include "hash2.h"
#include "testing.h"


#include "testfiles.h"
#include"query.h"
#include "join.h"

#define n 8

int main(int argc,char** argv){
	int i, numOffiles=0;
	char file[250], buff[400];
	batch* b=NULL;
	long int offset=0, prev_offset=0;
  	char init_file[20], work_file[40];
	RelFiles* relList=NULL;
	RelFiles* relCurr=relList;
	infoNode* infoMap=NULL;
 	FILE* fp;
   		
	/*scanf("%s", init_file);
	fp = fopen(init_file, "r");
	if(fp==NULL){fprintf(stderr, "Cannot open init_file \n"); return 1;}
	while(fscanf(fp, "%s", file)>0){
		
		numOffiles+=1;
	
	relCurr=add_Relation(&relList, relCurr, file);

	}
	//print_RelFiles( relList);
	//printf("total files %d\n",numOffiles );
	*/
 	fprintf(stderr, "Enter init file: \n" );
	while(1){
		if(	scanf("%s",file)==-1){fprintf(stderr, "scanf failed\n" ); return 1;}
		if(strcmp(file,"Done")==0 || strcmp(file,"done")==0)break;
		numOffiles+=1;
		//printf("%s\n",file );
		relCurr=add_Relation(&relList, relCurr, file);
	}
	infoMap=create_InfoMap(relList,  infoMap, numOffiles);
	//print_InfoMap( infoMap, numOffiles);


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
  	


  	fprintf(stderr,"Enter workload: \n");
  	execute_workload(numOffiles,infoMap);
	
	//fclose(fp);
	free_InfoMap(infoMap, numOffiles);
	return 0;
}
