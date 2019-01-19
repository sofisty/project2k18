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

//antigrafei mia lista apo predicates se mia kainourgia thn opoia epistrefei
pred* copy_predl( pred* predl){
	
	if(predl==NULL)return NULL;
	
	pred* head=(pred*)malloc(sizeof(pred) );
	head->rels[0]=predl->rels[0];
	head->rels[1]=predl->rels[1];
	head->cols[0]=predl->cols[0];
	head->cols[1]=predl->cols[1];
	head->isFilter=0;
	head->isSelfjoin=0;
	head->op=3;

	head->next=copy_predl(predl->next);
	return head;
	
		
}

//prosthetei ena predicate se mia lista apo predicates
 void add_pred(pred** head, pred* new_pred){
	pred* curr=*head;
	
	if(*head==NULL){
		*head=(pred*)malloc(sizeof(pred));
		(*head)->rels[0]=new_pred->rels[0];
		(*head)->rels[1]=new_pred->rels[1];
		(*head)->cols[0]=new_pred->cols[0];
		(*head)->cols[1]=new_pred->cols[1];
		(*head)->isFilter=0;
		(*head)->isSelfjoin=0;
		(*head)->op=3;

		(*head)->next=NULL;
		return;
		
	}


	while(curr->next!=NULL){
		curr=curr->next;	

	}
	curr->next=(pred*)malloc(sizeof(pred));
	curr->next->rels[0]=new_pred->rels[0];
	curr->next->rels[1]=new_pred->rels[1];
	curr->next->cols[0]=new_pred->cols[0];
	curr->next->cols[1]=new_pred->cols[1];
	curr->next->isFilter=0;
	curr->next->isSelfjoin=0;
	curr->next->op=3;
	curr->next->next=NULL;
	

}


//antigrafei mia lista apo predicates kai thn prosthetei sto telos mias hdh uparkths listas
void add_predl(pred** head, pred* new_pred){
	pred* curr=*head;
	
	if( (*head)->next==NULL){(*head)->next= copy_predl( new_pred); return;}

	while(curr->next!=NULL){curr=curr->next;}

	curr->next= copy_predl( new_pred);
	
}

//antikathista ena predicate se mia lista me ena allo
 void replace_pred(pred** head, pred* new_pred){
	pred* curr=*head;

	while(curr->next!=NULL){
		curr=curr->next;	

	}

	curr->rels[0]=new_pred->rels[0];
	curr->rels[1]=new_pred->rels[1];
	curr->cols[0]=new_pred->cols[0];
	curr->cols[1]=new_pred->cols[1];
	curr->next=NULL;	

}

//ektupwnei thn lista apo predicates
void print_predList(pred* head){
	pred* curr=head;
	int i =0;
	printf("----------------\n");
	while(curr!=NULL){
		printf("Pred %d : %d.%d & %d.%d \n", i, curr->rels[0], curr->cols[0],curr->rels[1], curr->cols[1]);
		i++;
		curr=curr->next;
	}

}

void free_predl(pred* head){
	pred* curr=head, *temp;
	while(curr!=NULL){
		temp=curr;
		curr=curr->next;
		free(temp);
	}
}

//ektupwnei to joinTree
void print_joinTree(treeNode* joinTree, int size){
	int i;
	for(i=0; i<size; i++){
		if(joinTree[i].path_stats==NULL){
			printf("Tree[%d] = x\n",i );
		}
		else{
			printf("Tree[%d] = ",i );
			print_predList(joinTree[i].predl);
			printf("\t--Cost %lf\n",joinTree[i].cost );
		}
	}
}

void free_joinTree(treeNode* joinTree, int size, int numOfrels){
	int i;
	for(i=0; i<size; i++){
		if(joinTree[i].path_stats!=NULL){
			free_stats(joinTree[i].path_stats, numOfrels);
			
		}
		if(joinTree[i].predl!=NULL){
			free_predl(joinTree[i].predl);
			

		}
	}
	free(joinTree);
	joinTree=NULL;
}


//vriskei thn veltisth seira ekteleshs twn joins kai epistrefei mia taksinomhmenh lista me ta predicates pros ektelesh
 pred* joinEnumeration(int numOfrels,  pred* predl, stats* qu_stats, treeNode** joinT){
	int numOfpreds, i, s, j, size, rel1, rel2, col1, col2, r_new,pos;
	pred* curr_pred=predl, *pred_head;
	treeNode* joinTree=*joinT;
	stats* temp=NULL;
	double cost;

	size= 1 << numOfrels; //2^n to megethos tou join tree
	
	//arxikopoihsh tou dentrou, apothhkeuontai ta predicates stis theseis tou dentrou gia 2 sxeseis 
	numOfpreds=0;
	while(curr_pred!=NULL){
		rel1=curr_pred->rels[0];
		rel2=curr_pred->rels[1];
		col1=curr_pred->cols[0];
		col2=curr_pred->cols[1];

		temp=copy_stats(qu_stats, numOfrels);
		update_joinStats(&temp[rel1], &temp[rel2],col1 ,col2);
		cost= temp[rel1].f[col1];

		pos=1 << rel1 | 1 << rel2; //thesh tou joinTree opou ta bit stis theseis rel1 kai rel2 einai 1
		
		if(joinTree[pos].path_stats==NULL ){ 	//den exei apothhkeutei allo predicate se authn thn thesh
			
			joinTree[pos].path_stats=copy_stats(temp, numOfrels);
			add_pred(	&(joinTree[pos].predl) ,curr_pred);
			joinTree[pos].cost=cost;
			
		}
		else{ //prostithetai to predicate sthn lista (periptwsh pou uparxoun panw apo ena predicate pou kanoun join me tis idies sxeseis)
			free_stats(joinTree[pos].path_stats, numOfrels);
			joinTree[pos].path_stats=copy_stats(temp, numOfrels);
			add_pred(	&(joinTree[pos].predl) ,curr_pred);
			joinTree[pos].cost+=cost;

		}
	
		joinTree[pos].path= joinTree[pos].path | 1 << numOfpreds; //apothhkeuei poia predicates exoun apothhkeutei sthn thesh pos
			
		
		free_stats(temp, numOfrels);
		curr_pred=curr_pred->next;
		numOfpreds++;
	}

	
	//-----------------------------------------------------------
	for(i=2; i<numOfrels; i++){ //ksekinaei apo upsos 2 logw ths arxikopoihshs tou joinTree 
	
		for(s=1; s<size; s++){ 
			
			if( __builtin_popcount(s)!=i )continue; //height
				
			for(j=1; j<size; j++){
				if( __builtin_popcount(j)!=2 || j==s )continue; // ta predicates epilegontai apo thn arxikopoihsh tou joinTree 
																//stis theseis s pou aforoun 2 sxeseis
				if(joinTree[s].path_stats==NULL){ continue;} //an den uparxoun apotthkeumena predicates stis theseis j kai s
				if(joinTree[j].path_stats==NULL){ continue;}

				curr_pred=joinTree[j].predl;
				rel1=curr_pred->rels[0];
				rel2=curr_pred->rels[1];
				
				if( ! (1<<rel1 & s || 1<<rel2 & s )){continue;} //connected
				if( 1<<j & joinTree[s].path){ continue;} //predicate hdh sto path 
				
				if(1<<rel1 &s ){r_new=rel2;}
				else{r_new=rel1;}
				
				temp=copy_stats(joinTree[s].path_stats, numOfrels);
				cost=0;
				pred_head=joinTree[j].predl;
				while(curr_pred!=NULL){ //parapanw apo ena predicate gia join me autes tis sxeseis

					col1=curr_pred->cols[0];
					col2=curr_pred->cols[1];
					
		
					update_joinStats(&temp[rel1], &temp[rel2],col1 ,col2);
					cost+= temp[rel1].f[col1]; //prostithetai to kostos
					curr_pred=curr_pred->next;
				
				}
				
				pos=s | 1<< r_new;
					
				if(joinTree[pos].path_stats==NULL || joinTree[pos].cost>joinTree[s].cost+cost){ //palio kostos sthn thesh pos > tou kostous gia to s + to kainourgio

					
					if(joinTree[pos].predl!=NULL){
						free_predl(joinTree[pos].predl);
						joinTree[pos].predl=NULL;
					}
					if(joinTree[pos].path_stats!=NULL){
						free_stats(joinTree[pos].path_stats, numOfrels);
						joinTree[pos].path_stats=NULL;
					}

					joinTree[pos].path_stats=copy_stats(temp, numOfrels);
					joinTree[pos].predl= copy_predl( joinTree[s].predl);
					
					add_predl( &(joinTree[pos].predl),pred_head);//prosthetei to kainourgio predicate
															
					joinTree[pos].cost=cost+joinTree[s].cost;
					joinTree[pos].path= joinTree[pos].path | 1 << joinTree[j].path;
				}
				
				free_stats(temp, numOfrels);
				
				
			}

		}
		

	}

	
	return joinTree[size-1].predl; //epistrefei thn lista apothhkeumenh sthn thesh pou ola ta bits einai 1
}

