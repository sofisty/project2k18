#include "optim.h"

/*int factorial(int n){
  return (n == 1 || n == 0) ? 1 : factorial(n - 1) * n;
}*/

stats* copy_stats(stats* qu_stats, int numOfrels){
	int i, clmns;
	stats* new_stats=(stats*)malloc(numOfrels* sizeof(stats));
	for(i=0; i<numOfrels; i++){
		clmns=qu_stats[i].columns;
		new_stats[i].columns=clmns;
		//printf("REl %d, i: %d with columns %d \n",r,i,clmns );

		new_stats[i].u=(uint64_t*)malloc(clmns* sizeof(uint64_t));
		memcpy(	new_stats[i].u,qu_stats[i].u, clmns*sizeof(uint64_t));
		
		new_stats[i].l=(uint64_t*)malloc(clmns* sizeof(uint64_t));
		memcpy(new_stats[i].l,qu_stats[i].l, clmns*sizeof(uint64_t));
		
		new_stats[i].d=(double*)malloc(clmns* sizeof(double));
		memcpy(new_stats[i].d,qu_stats[i].d, clmns*sizeof(double));
		
		new_stats[i].f=(double*)malloc(clmns* sizeof(double));
		memcpy(new_stats[i].f,qu_stats[i].f,  clmns*sizeof(double));
	}
	return new_stats;
	
}


double createJoinTRee(treeNode currTree, treeNode* newTree, int rel1, int rel2,joinHash* jh, int k , int numOfrels){
	int i, col1, col2;
	double cost=0;

	newTree->path_stats= copy_stats(currTree.path_stats, numOfrels);

	printf("buck count %d \n", jh->buckCount[k]);
	for(i=0; i<jh->buckCount[k]; i++){
		col1=jh->bucketArr[k][i].new_cols[0];
		col2=jh->bucketArr[k][i].new_cols[1];
		printf("REL1 %d  COL1 %d  & REL2 %d  COL2 %d and k %d\n",rel1, col1, rel2, col2,k );

		printf("-----\tcost before rel1 : %lf rel2: %lf \n", newTree->path_stats[rel1].f[col1] ,newTree->path_stats[rel1].f[col1] );
		update_joinStats( &(newTree->path_stats[rel1]),&(newTree->path_stats[rel1]),col1, col2);
		printf("\tcost after rel1 : %lf rel2: %lf ------- \n", newTree->path_stats[rel1].f[col1] ,newTree->path_stats[rel1].f[col1] );

	}
	cost= newTree->path_stats[rel1].f[col1];

	return cost;
}


int* joinEnumeration(int numOfrels, joinHash* jh, stats* qu_stats){
	int pos, posS,posJ;
	int i, j, k, s,size=1 << numOfrels;
	int checked=0;
	//printf("SIZE IS %d and numOfrels %d \n",size,numOfrels );
	int best=0, curr=0;
	treeNode* joinTree=(treeNode*)malloc(size  *sizeof(treeNode));
	if (joinTree==NULL) {fprintf(stderr, "Malloc failed \n"); exit(-1);}
	
	for(i=0;i<size;i++){
		joinTree[i].path=(int*)malloc(numOfrels* sizeof(int));
		if (joinTree[i].path==NULL) {fprintf(stderr, "Malloc failed \n"); exit(-1);}
		memset(joinTree[i].path, 0, numOfrels* sizeof(int));
	}

	for(i=0; i<numOfrels; i++){		
		pos= 1 << i;
		//printf(" pos %d  and %d \n",pos, i );
		joinTree[pos].path[0]=i;
		joinTree[pos].path_stats=copy_stats(qu_stats, numOfrels);
		//print_stats(joinTree[pos].path_stats, numOfrels);
	}
	
	k=0;
	for(i=1; i<numOfrels; i++){
			
		for(s=0; s<numOfrels; s++ ){
			if(i==1)curr = 1 << s ;
			pos=curr;
			printf("________S %d \n",s );	
			
			for(j=0; j<numOfrels; j++){
				printf("________J %d \n",j );	

				if(best & 1 << j ){ continue;}
				k= relCombHash(s, j, jh->relCombs, jh->numOfcombs);
				if(k==-1)continue;
				if(jh->bucketArr[k]==NULL){ printf("x %d \n",k );continue;}
				
				posJ=1 << j;
				
				curr=curr | posJ;
				//printf("POS: %d me relS %d kai relJ %d\n",pos, s,j );
				memcpy(joinTree[curr].path, joinTree[pos].path, i * sizeof(int)) ;
				

				joinTree[curr].path[i]=j;
		
				joinTree[curr].cost=createJoinTRee(	joinTree[pos], &joinTree[curr] ,s, j, jh,k, numOfrels);
				if(best==0 || joinTree[best].cost>joinTree[curr].cost){
					best=curr;
					
				}			
				
			}

			curr=best;
			
			printf("BEST TREE UNTIL NOW WITH HIGHT: %d STORED POS: %d \n",i, best);
			for(int g=0; g<i; g++){printf("~ %d ",joinTree[best].path[g]);}
			printf("\n");

		}
		curr=best;
		
		
	}

	exit(0);
	return NULL;	
}

/*

int main(void){

	int i, combinations;
	joinTree_node root;
	root.degree=0;
	root.path=NULL;
	root.parent=NULL;
	root.child=NULL;

	joinIndex index;
	index.numOfrels=4;
	index.posOfJoins=malloc(index.numOfrels*(sizeof(int*)));
	if (index.posOfJoins==NULL) {
    	fprintf(stderr, "Malloc failed \n"); 
    	exit(-1);
	}
	for(i=0;i<index.numOfrels;i++){
		index.posOfJoins[i]=malloc(index.numOfrels*sizeof(int));
		if (index.posOfJoins[i]==NULL) {
	    	fprintf(stderr, "Malloc failed \n"); 
	    	exit(-1);
		}
	}

	
	combinations=factorial(index.numOfrels)/((factorial(index.numOfrels-2))*2);
	index.relCombs=malloc(combinations*sizeof(int*));
	if (index.relCombs==NULL) {
    	fprintf(stderr, "Malloc failed \n"); 
    	exit(-1);
	}

	//edw prospathe na ftiaksw kati tetoio [0,1][0,2][0,3][1,2][1,3][2,3] pou einai oloi oi pihtanoi sindiasmoi xwris epanalipseis 
	//alla exei kaei o egkefalos mou opote den borw na grapse ti loopa tha ta dw avrio me katharotero mialo

	for(i=0;i<combinations;i++){
		while(j<index.numOfrels){
			index.relCombs[i][0]=stable_rel;
			index.relCombs[i][1]=
		}
	}

	index.joinBuckets=malloc(combinations*sizeof(pred*));
	if (index.joinBuckets==NULL) {
    	fprintf(stderr, "Malloc failed \n"); 
    	exit(-1);
	}

	//tha parw to paradeigma me relations 0,1,2,3 (pseutika rels) kai joins 0.0=2.0 1.0=2.2 1.3=0.2 3.1=1.1



}
*/