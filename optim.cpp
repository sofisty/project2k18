#include "optim.h"


//antigrafei enan array numOrels grammwn apo stats se enan kaiourgio, to opoio kai epistrefei 
stats* copy_stats(stats* qu_stats, int numOfrels){
	int i, clmns;
	stats* new_stats=(stats*)malloc(numOfrels* sizeof(stats));
	for(i=0; i<numOfrels; i++){
		clmns=qu_stats[i].columns;
		new_stats[i].columns=clmns;

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


//upologizei thn veltisth seira ekteleshs twn joins kai enhmerwnei thn proteraiothta tou predicate
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

	//epilegetai poio predicate exei ta ligotera apotelesmata gia na ektelestei prwto
	numOfpreds=0;
	while(curr_pred!=NULL){
		rel1=curr_pred->rels[0];
		rel2=curr_pred->rels[1];
		col1=curr_pred->cols[0];
		col2=curr_pred->cols[1];

		temp=copy_stats(qu_stats, numOfrels);
		update_joinStats(&temp[rel1], &temp[rel2],col1 ,col2);
		cost= temp[rel1].f[col1];

		if(best_stats==NULL || bestCost>cost){
			bestCost=cost;
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
	//int s[numOfrelations]: krataei tis sxeseis twn predicates pou exoun epilegei na ektelestoun mexri twra
	s[0]=best_rel1;
	s[1]=best_rel2;
	//int hash_checked[numOfrelations]: parallhlos me ton s kai krataei gia thn sxesh sthn thesh i tou s pinaka,
	// enan int, pou dhlwnei oles tis prohgoumenes sxeseis ston s pinaka
	//gia kathe prohgoumenh sxesh h antistoixh thesh bit ston int ginetai 1, dhladh to prwto relation antistoixei sto prwto bit klp
	hash_checked[0]=0;
	hash_checked[1]=1 << best_rel1;
	//poses sxeseis exoun ta epilegmena predicates mexri twra 
	height =2;
	best_pred->priority=prior;
	//int pou dhlwnei poies sxeseis exoun epilegei, to bit tou int pou upodhlwnei thn sxesh ginetai 1
	hash_code=1 << best_rel1 | 1 <<best_rel2;

	bestOfH=copy_stats(best_stats, numOfrels);
	//gia ola ta predicates pou den exoun epilegei 
	for(i=0; i<numOfpreds-1; i++){
		free_stats(best_stats, numOfrels); 
		best_stats=NULL;
		bestCost=0;
		
		start_stats=bestOfH;
		//anazhthsh gia ta predicates pou exoun sxeseis sto s 
		for(j=0; j<height; j++){
			curr_pred =*predl;
			while(curr_pred!=NULL){
				rel1=curr_pred->rels[0] ;
				rel2=curr_pred->rels[1] ;
				col1=curr_pred->cols[0];
				col2=curr_pred->cols[1];

				if( (s[j]==rel1 || s[j]==rel2) && curr_pred->priority==0){
			
					//apofygh twn diplwn elegxwn twn predicates
					if(s[j]==rel1){
						//an to bit sto hash_checked[j] einai 1 gia thn sxesh rel2
						// shmainei oti exoun upologistei se prohgoumeno relation apo ton s ta predicates me sunduasmo rel1-rel2  
						if( hash_checked[j] & (1 << rel2)){
							curr_pred=curr_pred->next;
							continue;
						}
					}
					if(s[j]==rel2){
						if( hash_checked[j] & (1 << rel1 )){
							curr_pred=curr_pred->next;
							continue;
						}
					}
					
					temp=copy_stats(start_stats, numOfrels);
					update_joinStats(&temp[rel1], &temp[rel2],col1 ,col2);
					cost= temp[rel1].f[col1];

					
					if(best_stats==NULL || bestCost>cost){
						bestCost=cost;
						
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
						
					}
					free_stats(temp, numOfrels);
					
				}
				
				curr_pred=curr_pred->next;

			}
			
		}
	
		if(bestOfH!=NULL)free_stats(bestOfH, numOfrels);
		bestOfH=copy_stats(best_stats, numOfrels);
		if( !( hash_code & (1 << best_rnew) ) ){ //an to rnew den uparxei sto s 
			s[height]=best_rnew; //prostithetai h kainourgia sxesh
			hash_checked[height]= hash_checked[height - 1 ] | ( 1 << s[height-1] ) ; //enhmerwnetai to hash_checked
			height+=1; //auksanetai to upsos
			hash_code= hash_code | 1 << best_rnew ; //prostithetai h kainourgia sxesh
			
		}
		//enhmerwnetai h proteraiothta tou predicate
		prior+=1;
		best_pred->priority=prior;
		//printf("Prioir: %d Join: rel1 %d col1 %d rel2 %d col2 %d \n", prior ,best_pred->rels[0], best_pred->cols[0], best_pred->rels[1], best_pred->cols[1]);
	

	}
	if(best_stats!=NULL){free_stats(best_stats, numOfrels);}
	if(bestOfH!=NULL){free_stats(bestOfH, numOfrels);}
	free(s);
	free(hash_checked);
	return 0;	
}

