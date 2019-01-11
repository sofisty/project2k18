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
		//printf("REl %d, i: %d with columns %d \n",i,i,clmns );

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



int joinEnumeration(int numOfrels, pred** predl, stats* qu_stats){
	pred* curr_pred =*predl, *best_pred;
	int i =0,j=0,height ,rel1, rel2, col1, col2, best_rel1, best_rel2, best_rnew ,numOfpreds, prior=1, hash_code;
	int* s, *hash_checked;
	double cost=0, bestCost=0;
	stats* temp=NULL, *bestOfH=NULL, *best_stats=NULL, *start_stats=NULL;
	s=(int*)malloc(numOfrels* sizeof(int));
	if(s==NULL){fprintf(stderr, "Malloc failed\n"); exit(1);}
	hash_checked=(int*)malloc(numOfrels* sizeof(int));
	if(hash_checked==NULL){fprintf(stderr, "Malloc failed\n"); exit(1);}

	numOfpreds=0;
	while(curr_pred!=NULL){
		rel1=curr_pred->rels[0];
		rel2=curr_pred->rels[1];
		col1=curr_pred->cols[0];
		col2=curr_pred->cols[1];

		temp=copy_stats(qu_stats, numOfrels);
		update_joinStats(&temp[rel1], &temp[rel2],col1 ,col2);
		cost= temp[rel1].f[col1];

		//printf("Join %d=%d curr_cost %lf , best_cost %lf\n",rel1, rel2,cost,bestCost );
		if(best_stats==NULL || bestCost>cost){
			bestCost=cost;
			//printf("\t new Best %lf\n",bestCost );
			if(best_stats !=NULL){ 
				free_stats( best_stats, numOfrels);
				best_stats=copy_stats(temp, numOfrels);
			}
			else{
				best_stats=copy_stats(temp, numOfrels);
			}
			best_pred=curr_pred;

			best_rel1=rel1;
			best_rel2=rel2;
		}
		free_stats(temp, numOfrels);
		curr_pred=curr_pred->next;
		numOfpreds++;
	}
	s[0]=best_rel1;
	hash_checked[0]=0;
	s[1]=best_rel2;
	hash_checked[1]=1 << best_rel1;
	height =2;
	best_pred->priority=prior;
	hash_code=1 << best_rel1 | 1 <<best_rel2;


	//printf("-------------END OF INITIALIZATION-------------\n");
	//printf("PREDS %d\n",numOfpreds );
	//printf("BEST COST %lf\n",bestCost );
	//printf("HASH CODE %d\n",hash_code );
	//printf("RELATIONS: %d CHECKED %d + %d CHECKED %d\n",s[0],hash_checked[0], s[1],hash_checked[1] );

	bestOfH=copy_stats(best_stats, numOfrels);
	for(i=0; i<numOfpreds-1; i++){
		free_stats(best_stats, numOfrels); //arxikopoiountai ta kainourgia kalutera
		best_stats=NULL;
		bestCost=0;
		
		start_stats=bestOfH;
		for(j=0; j<height; j++){
			curr_pred =*predl;
			while(curr_pred!=NULL){
				rel1=curr_pred->rels[0] ;
				rel2=curr_pred->rels[1] ;
				col1=curr_pred->cols[0];
				col2=curr_pred->cols[1];

				if( (s[j]==rel1 || s[j]==rel2) && curr_pred->priority==0){
					//printf("--MPHKA GIA s[j] %d kai rel1 %d rel2 %d\n",s[j],rel1,rel2 );
					if(s[j]==rel1){
						if( hash_checked[j] & (1 << rel2)){
							//printf("EXIT gia checked %d me rel2 %d\n", hash_checked[j], rel2);
							curr_pred=curr_pred->next;
							continue;
						}
					}
					if(s[j]==rel2){
						if( hash_checked[j] & (1 << rel1 )){
							//printf("EXIT gia checked %d me rel1 %d\n", hash_checked[j], rel1);
							curr_pred=curr_pred->next;
							continue;
						}
					}
					
					temp=copy_stats(start_stats, numOfrels);
					update_joinStats(&temp[rel1], &temp[rel2],col1 ,col2);
					cost= temp[rel1].f[col1];

					//printf("Join %d=%d curr_cost %lf , best_cost %lf\n",rel1, rel2,cost,bestCost );
					if(best_stats==NULL || bestCost>cost){
						bestCost=cost;
						//printf("\t new Best %lf\n",bestCost );
						if(best_stats !=NULL){ 
							free_stats( best_stats, numOfrels);
							best_stats=copy_stats(temp, numOfrels);
						}
						else{
							best_stats=copy_stats(temp, numOfrels);
						}
						best_pred=curr_pred;
						if(s[j]==rel1){best_rnew=rel2; }
						else{ best_rnew=rel1;}
						//printf("best r new %d \n",best_rnew );
					}
					free_stats(temp, numOfrels);
					
				}
				//else{printf("den vrhka allo query hash_code %d\n", hash_code);}
				curr_pred=curr_pred->next;

			}
			
		}
		//else{printf("kompleeeeee\n");}
		if(bestOfH!=NULL)free_stats(bestOfH, numOfrels);
		bestOfH=copy_stats(best_stats, numOfrels);
		if( !( hash_code & (1 << best_rnew) ) ){ //an to rnew den uparxei sto s 
			s[height]=best_rnew;
			hash_checked[height]= hash_checked[height - 1 ] | ( 1 << s[height-1] ) ; //krataei ton prohgoumeno hash code kai to s prin to rnew
			height+=1;
			hash_code= hash_code | 1 << best_rnew ; //to monopati mexri twra 
			//printf("NEW HASH CODE %d\n",hash_code );

		}
		prior+=1;
		best_pred->priority=prior; 

	}
	if(best_stats!=NULL){free_stats(best_stats, numOfrels);}
	if(bestOfH!=NULL){free_stats(bestOfH, numOfrels);}
	free(s);
	free(hash_checked);
	return 0;	
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