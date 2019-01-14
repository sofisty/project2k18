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

pred* copy_predl( pred* predl){
	
	if(predl==NULL)return NULL;
	
	pred* head=(pred*)malloc(sizeof(pred) );
	head->rels[0]=predl->rels[0];
	head->rels[1]=predl->rels[1];
	head->cols[0]=predl->cols[0];
	head->cols[1]=predl->cols[1];
	head->isFilter=0;
	head->isSelfjoin=0;
	head->next=copy_predl(predl->next);
	return head;
	
		
}
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
	curr->next->next=NULL;
	

}

void add_predl(pred** head, pred* new_pred){
	pred* curr=*head;
	
	if( (*head)->next==NULL){(*head)->next= copy_predl( new_pred); return;}

	while(curr->next!=NULL){curr=curr->next;}

	curr->next= copy_predl( new_pred);
	
}

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

 pred* joinEnumeration(int numOfrels,  pred* predl, stats* qu_stats){
	int numOfpreds, i, s, j, size, rel1, rel2, col1, col2, r_new,pos;
	pred* curr_pred=predl, *pred_head;
	treeNode* joinTree;
	stats* temp=NULL, *best_stats=NULL;
	double cost;

	// __builtin_popcount (unsigned int x)

	size= 1 << numOfrels;
	printf("SIZE IS %d \n",size );
	joinTree= (treeNode*) malloc(size* sizeof(treeNode));
	for(i=0; i<size; i++){
		joinTree[i].path_stats=NULL;
		joinTree[i].predl=NULL;
		joinTree[i].path=0;
	}

	numOfpreds=0;
	while(curr_pred!=NULL){
		rel1=curr_pred->rels[0];
		rel2=curr_pred->rels[1];
		col1=curr_pred->cols[0];
		col2=curr_pred->cols[1];

		temp=copy_stats(qu_stats, numOfrels);
		update_joinStats(&temp[rel1], &temp[rel2],col1 ,col2);
		cost= temp[rel1].f[col1];

		pos=1 << rel1 | 1 << rel2;
		
		if(joinTree[pos].path_stats==NULL ){	
			
			joinTree[pos].path_stats=copy_stats(temp, numOfrels);
			add_pred(	&(joinTree[pos].predl) ,curr_pred);
			print_predList(joinTree[pos].predl);
			joinTree[pos].cost=cost;
			
		}
		else{
			printf("REPLACE \n");
			free_stats(joinTree[pos].path_stats, numOfrels);
			joinTree[pos].path_stats=copy_stats(temp, numOfrels);
			//replace_pred( &(joinTree[pos].predl), curr_pred);
			add_pred(	&(joinTree[pos].predl) ,curr_pred);
			print_predList(joinTree[pos].predl);
			joinTree[pos].cost+=cost;

		}
	
		joinTree[pos].path= joinTree[pos].path | 1 << numOfpreds;
			
		
		free_stats(temp, numOfrels);
		curr_pred=curr_pred->next;
		numOfpreds++;
	}

	printf("END OF INIT\n");
	print_joinTree( joinTree,  size);

	//-----------------------------------------------------------
	for(i=2; i<numOfrels; i++){
	
		for(s=1; s<size; s++){
			printf("--S %d\n",s);
			if( __builtin_popcount(s)!=i )continue; //height
				
			for(j=1; j<size; j++){
				if( __builtin_popcount(j)!=2 || j==s )continue; //height
				if(joinTree[s].path_stats==NULL){ continue;}
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
				while(curr_pred!=NULL){
					//printf("MPHKA WHILE\n");
					col1=curr_pred->cols[0];
					col2=curr_pred->cols[1];
					printf("\t J %d| %d.%d & %d.%d \n",j,rel1,col1,rel2,col2 );
		
					update_joinStats(&temp[rel1], &temp[rel2],col1 ,col2);
					cost+= temp[rel1].f[col1];
					curr_pred=curr_pred->next;
				
				}
				
				pos=s | 1<< r_new;
				printf("POS %d \n",pos );
				if(joinTree[pos].path_stats!=NULL){
					printf("SUGKRINW cost now %lf me join tree %lf \n",joinTree[s].cost+cost, joinTree[pos].cost);
				} 
				if(joinTree[pos].path_stats==NULL || joinTree[pos].cost>joinTree[s].cost+cost){

					
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
					//print_predList(joinTree[pos].predl);
					add_predl( &(joinTree[pos].predl),pred_head);
					print_predList(joinTree[pos].predl);
															
					joinTree[pos].cost=cost+joinTree[s].cost;
					joinTree[pos].path= joinTree[pos].path | 1 << joinTree[j].path;
				}
				
				free_stats(temp, numOfrels);
				
				
			}

		}
		

	}


	//print_joinTree( joinTree,  size);
	print_predList(joinTree[size-1].predl);
	
	return joinTree[size-1].predl;
}

