#include <unistd.h>
#include "hash1.h"
#include "hash2.h"

#include "infomap.h"
#include"query.h"
#include "join.h"

#define n 8

int main(void){
	int numOffiles=0;
	char file[250];
	RelFiles* relList=NULL;
	RelFiles* relCurr=relList;
	infoNode* infoMap=NULL;
   		
	
 	//fprintf(stderr, "Enter relation file: \n" );
 	//oso dinwtai apo to stdin arxeio
	while(1){
		if(	scanf("%s",file)==-1){fprintf(stderr, "scanf failed\n" ); return 1;}

		//ean dothei to alfarithmitiko "done" tote teleiwnei h eisodos apo to stdin
		if(strcmp(file,"Done")==0 || strcmp(file,"done")==0)break; 
		numOffiles+=1; //auksanontai ta arxeia relations kata ena
		relCurr=add_Relation(&relList, relCurr, file); //eisagw to relation sth lista me ta relations
	}

	//dimiourgw to infomap apo ta dosmena relations
	infoMap=create_InfoMap(relList,  infoMap, numOffiles);

  	//fprintf(stderr,"Enter query: \n");
  	execute_workload(numOffiles,infoMap); //ektelei to workload pou dinetai se  batches  apo to stdin
	
  	//apodesmevei ton xwro pou eixe desmevtei gia to infoMap
	free_InfoMap(infoMap, numOffiles);
	return 0;
}
