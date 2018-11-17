#include "query.h"

//metraei ta queries enos batch
int count_Qnum(FILE* fp,long int* offset){ 
	int num_queries=0,ch=0;
	
	while(!feof(fp)){
		ch = fgetc(fp);
		(*offset)++;
		if((ch=='F')){
			ch = fgetc(fp);
			(*offset)++;
			break;
		} 
		if(ch == '\n') 	num_queries++;

	}

	return num_queries;
}
//metraei ta predicates enos query
int count_preds(char* preds){
	char* token, *temp;
	int num_preds=0;
	temp=malloc(((strlen(preds))+1)*sizeof(char));
	if (temp== NULL) {
	    fprintf(stderr, "Malloc failed \n"); 
	    exit(-1);
	}
	strcpy(temp, preds);


	token= strtok(temp,"&"); 
	while(token!=NULL){
		num_preds++;
		token= strtok(NULL,"&");
	}
	free(temp);
	return num_preds;
}

//metraei ta relations pou simetexoun sto query
int count_rels(char* rels){
	char* token, *temp;
	int num_rels=0;
	temp=malloc(((strlen(rels))+1)*sizeof(char));
	if (temp== NULL) {
	    fprintf(stderr, "Malloc failed \n"); 
	    exit(-1);
	}
	strcpy(temp, rels);


	token= strtok(temp," "); 
	while(token!=NULL){
		num_rels++;
		token= strtok(NULL," ");
	}
	free(temp);
	return num_rels;
}

//apothikevei ena pred sth thesh pou prepei sth lista predicates tou query
void store_pred(char* pred_str, pred* p, pred* prev){
	int i,dots=0, index,rel1,rel2;
	char *token, *predicate, delim[]="><=";
	
	//to orizw edw gia na to allaksw an xreiastei
	p->isFilter=0;
	p->isSelfjoin=0;

	predicate=malloc(((strlen(pred_str))+1)*sizeof(char));
	if (predicate== NULL) {
	    fprintf(stderr, "Malloc failed \n"); 
	    exit(-1);
	}
	strcpy(predicate, pred_str);

	//printf(".........%s\n", predicate);
	//desmevw xwro gia ta columns estw oti to pred einai 1.22>3.2 tote desmevw strlen-2 ,diladi afairw ta dots .ta cols diaxwrizontai me -1
	p->cols=malloc((strlen(predicate)-1)*sizeof(int)); //enw ta dots einai 2 , alla iparxei kai -1 sto telos opote strlen-1 
	if (p->cols== NULL) {
	    fprintf(stderr, "Malloc failed \n"); 
	    exit(-1);
	}
	//kanw tokenize to kathgorima
	token= strtok(predicate,delim);  //pairnw to prwto tmhma tou kathgorhmatos pou einai sxedon panta mia sthlh enos relation
	if(token==NULL){
		printf("Not apropriate format for a predicate.\n");
		exit(-1);
	}
	for(i=0;i<strlen(token);i++){
		if(token[i]=='.') dots++;
	}
	if(dots==1){ //apothikevw ta columns tou predicate
		index=0;
		rel1=token[0] - '0';
		for(i=0;i<strlen(token);i++){
			if(token[i]!='.'){
				p->cols[index]=token[i] - '0';
				//printf("---------%d\n",p->cols[index]);
				index++;
			}
		}
		p->cols[index]=-1; //diaxwrhstiko twn columns
		index++;
	}
	else {
		p->val=atoi(token);
		p->isFilter=1;
	}

	//deutero tmhma tou kathgorhmatos
	dots=0;
	token= strtok(NULL,delim); 
	if(token==NULL){
		printf("Not apropriate format for a predicate.\n");
		exit(-1);
	}
	for(i=0;i<strlen(token);i++){
		if(token[i]=='.') dots++;
	}
	if(dots==1){
		rel2=token[0] - '0';
		//twra tha elegksw an to kathgorima einai selfjoin
		if(rel1==rel2) p->isSelfjoin=1;

		for(i=0;i<strlen(token);i++){
			if(token[i]!='.'){
				p->cols[index]=token[i] - '0';
				//printf("---------%d\n",p->cols[index+2]);
				index++;
			}
		}
		p->cols[index]=-1; //diaxwrhstiko twn columns
	}
	else {
		p->val=atoi(token);
		p->isFilter=1;
	}

	//twra tha apothikefsw ton operator tou kathgorhmatos 1 gia >, 2 gia < kai 3 gia =
	i=0;
	while((pred_str[i]!='>')&&(pred_str[i]!='<')&&(pred_str[i]!='=')) i++;
	if(pred_str[i]=='>') p->op=1;
	else if(pred_str[i]=='<') p->op=2;
	else p->op=3;

	if(prev!=NULL) prev->next=p;
	//else printf("This is the first predicate of this query!\n");
	p->next=NULL;

	free(predicate);
}

//apothikevei ta projections, idia domi me ta columns
void store_proj(char* proj_str, query* q){
	int i,dots=0, index;
	char *token, *projections;

	int* proj=q->projs;
	
	//to orizw edw gia na to allaksw an xreiastei

	projections=malloc(((strlen(proj_str))+1)*sizeof(char));
	if (projections== NULL) {
	    fprintf(stderr, "Malloc failed \n"); 
	    exit(-1);
	}
	strcpy(projections, proj_str);

	//metraw posa einai ta projections gia na desmefsw xwro
	for(i=0;i<strlen(projections);i++){
		if(projections[i]=='.') dots++;
	}
	q->num_projs=dots;
	//printf("np %d\n",q->num_projs);

	proj=malloc((strlen(projections)-dots+1)*sizeof(int));
	if (proj== NULL) {
	    fprintf(stderr, "Malloc failed \n"); 
	    exit(-1);
	}

	token= strtok(projections," \n");//pairnw to prwto projection
	if(token==NULL){
		printf("Not apropriate format for a predicate.\n");
		exit(-1);
	}
	index=0;
	while(token!=NULL){
		for(i=0;i<strlen(token);i++){
			if(token[i]!='.'){
				proj[index]=token[i] - '0';
				//printf("%d--------\n",proj[index]);
				index++;
			}
		}
		proj[index]=-1;
		index++;
		token= strtok(NULL," \n");
	}

	q->projs=proj;
	
	free(projections);		
}

//taksinomw ti lista twn kathgorhmatwn me auto to tropo |filters|selfjoins|rest of predicates...
void reorder_preds(query* q){
	pred* head=q->preds;
	pred *temp, *prev, *endFilters, *curr;
	curr=head;
	prev=curr;
	//Vazw ta filtra sthn arxh ths listas
	while(curr!=NULL){
		if((curr->isFilter)&&(curr!=head)){
			prev->next=curr->next;
			temp=head;
			head=curr;
			head->next=temp;
		} 
		prev=curr;
		curr=curr->next;
	}
	//Vazw ta selfjoins amesws meta ta filtra sth lista
	q->preds=head;
	curr=head;
	prev=curr;
	while((curr->isFilter)){
		prev=curr;
		curr=curr->next;
	} 
	endFilters=curr;
	curr=endFilters;
	while(curr!=NULL){
		if(curr->isSelfjoin){
			prev->next=curr->next;
			temp=endFilters;
			endFilters=curr;
			endFilters->next=temp;

		}
		prev=curr;
		curr=curr->next;
	}
	print_query(*q);
}

void print_query(query q){
	int i,j;
	printf("The query uses the relations: \n");
	for(i=0;i<q.num_rels;i++){
		if(q.rels[i]!=-1){
			printf("%d  ",q.rels[i]);
		}
	}
	printf("\n\n");
	printf("The following predicates must be implemented: \n");
	pred* curr=q.preds;
	while(curr!=NULL){
		printf("%d.",curr->cols[0]);
		i=1;
		while(curr->cols[i]!=-1){
			printf("%d",curr->cols[i]);
			i++;
		}
		i++;
		printf(" "); 
		if(curr->op==1) printf("> ");
		else if(curr->op==2) printf("< ");
		else printf("= ");
		if (curr->isFilter) printf("%d", curr->val);
		else {
			printf("%d.",curr->cols[i]);
			i++;
			while(curr->cols[i]!=-1){
				printf("%d",curr->cols[i]);
				i++;
			}
		}
		printf("\n");
		curr=curr->next;
	}
	printf("\n");
	printf("The following columns must be projected: \n");
	i=0;
	for(j=0;j<q.num_projs;j++){
		printf("%d.",q.projs[i]);
		i++;
		while(q.projs[i]!=-1){
			printf("%d",q.projs[i]);
			i++;
		}
		i++;
		printf(" ");

	}
	printf("\n\n");	

}

//metraei ta batches enos file
int count_batches(FILE* fp){
	int num_batches=0, ch=0;

	while(!feof(fp)){
		ch=fgetc(fp);
		if((ch=='F')) num_batches++;
	}
	return num_batches;
}

query* store_query(char* qstr, query* q){
	int i=0,num_preds,num_rels;
	char *token, *rels, *preds, *projs;

	q=malloc(sizeof(query));
	if (q== NULL) {
	    fprintf(stderr, "Malloc failed \n"); 
	    exit(-1);
	}


	//spaw to qstr sta tria tmimata kai apothikevw ta tmimata se 3 diaforetika strings
	//RELATIONS
	token= strtok(qstr,"|"); 
	if(token==NULL){
		printf("Not apropriate format for a query.\n");
		exit(-1);
	}
	rels=malloc((strlen(token)+1)*sizeof(char));
	if (rels== NULL) {
	    fprintf(stderr, "Malloc failed \n"); 
	    exit(-1);
	}
	strcpy(rels, token);

	//PREDICATES
	token= strtok(NULL,"|");
	if(token==NULL){
		printf("Not apropriate format for a query.\n");
		exit(-1);
	}
	preds=malloc((strlen(token)+1)*sizeof(char));
	if (preds== NULL) {
	    fprintf(stderr, "Malloc failed \n"); 
	    exit(-1);
	}
	strcpy(preds, token);

	//PROJECTIONS
	token= strtok(NULL,"|");
	if(token==NULL){
		printf("Not apropriate format for a query.\n");
		exit(-1);
	}
	projs=malloc((strlen(token)+1)*sizeof(char));
	if (projs== NULL) {
	    fprintf(stderr, "Malloc failed \n"); 
	    exit(-1);
	}
	strcpy(projs, token);


	//Apothikevw sth domy query ta dedomena apo to arxeio
	//RELATIONS
	num_rels=count_rels(rels);
	q->rels=malloc(num_rels*sizeof(int));
	if (q->rels== NULL) {
	    fprintf(stderr, "Malloc failed \n"); 
	    exit(-1);
	}
	q->num_rels=num_rels;
	i=0;

	//apothikevw ta relations pou xrhsimopoiei to query sth domi
	token= strtok(rels," "); 
	while(token!=NULL){
		q->rels[i]=atoi(token);
		//printf("%d\n",q.rels[i]);
		token= strtok(NULL," ");
		i++;
	}

	//PREDICATES
	//printf("%s\n", preds );
	num_preds=count_preds(preds);

	q->preds=malloc(num_preds*sizeof(pred));
	i=0;
	if (q->preds == NULL) {
	    fprintf(stderr, "Malloc failed \n"); 
	    exit(-1);
	}

	//spaw ola ta kathgorhmata kai ta kratw ksexwrista
	char *predicates[num_preds];
	token= strtok(preds,"&"); //apothikevw ta relations pou xrhsimopoiei to query sth domi
	if(token==NULL){
		printf("Not apropriate format for a predicate.\n");
		exit(-1);
	}
	predicates[0]=malloc((strlen(token)+1)*sizeof(char));
	if (predicates[0] == NULL) {
	    fprintf(stderr, "Malloc failed \n"); 
	    exit(-1);
	}

	strcpy(predicates[0],token);
	//printf("%s\n", predicates[0] );

	for(i=1;i<num_preds;i++){
		token= strtok(NULL,"&"); //apothikevw ta relations pou xrhsimopoiei to query sth domi
		if(token==NULL){
			printf("Not apropriate format for a predicate.\n");
			exit(-1);
		}
		predicates[i]=malloc((strlen(token)+1)*sizeof(char));
		if (predicates[i] == NULL) {
		    fprintf(stderr, "Malloc failed \n"); 
		    exit(-1);
		}

		strcpy(predicates[i],token);
		//printf("%s\n", predicates[i] );

	}

	for(i=0;i<num_preds;i++){
		//printf("%s\n",predicates[i]);
		if(i==0) store_pred(predicates[i],&(q->preds[i]),NULL);
		else store_pred(predicates[i],&(q->preds[i]),&(q->preds[i-1]));

	}

	//PROJECTIONS
	store_proj(projs, q);	


	//free ta strings
	free(rels);
	free(preds);
	free(projs);

	//free ta predicates
	for(i=0;i<num_preds;i++){
		free(predicates[i]);
	}

	return q;

}

batch* store_batch(FILE* fp,long int* offset,long int* prev_offset,char* buff, int buff_size, batch* b){
	int num_queries,i=0;	

	num_queries=count_Qnum(fp,offset);
	printf("This batch has %d queries.\n",num_queries );
	fseek(fp,*prev_offset, SEEK_SET);

	//desmevw xwro gia to batch
	b=malloc(sizeof(batch));
	if (b== NULL) {
    	fprintf(stderr, "Malloc failed \n"); 
    	exit(-1);
	}

	b->q_arr=malloc(num_queries*sizeof(query*));
	if (b->q_arr== NULL) {
    	fprintf(stderr, "Malloc failed \n"); 
    	exit(-1);
	}
	b->num_queries=num_queries;

	i=0;
	while (fgets(buff,buff_size,fp)) { //pairnei grammi -grammi pou kathe grammi einai mia eggrafh
		if(strcmp(buff,"F\n")!=0){
			b->q_arr[i]=store_query(buff,b->q_arr[i]);
			reorder_preds(b->q_arr[i]);
			//print_query(*(b->q_arr[i]));
			i++;
		}
		else{
			//printf("				The batch is over!\n\n\n");
			break;
		}		
	}
	return b;
}

void print_batch(batch* b){
	int i;
	if(b==NULL){
		printf("This batch is empty.Exiting.\n");
		exit(-1);
	}
	for(i=0;i<b->num_queries;i++){
		print_query(*(b->q_arr[i]));	
	}
}
void free_query(query* q){
	query* curr=q;
	pred* temp;
	pred* currp=q->preds;

	while((currp!=NULL)&&(currp->next!=NULL)){
		temp=currp;
		currp=currp->next;
		free(temp->cols);
		free(temp);
		temp=NULL;
	}
	free(curr->rels);
	free(curr->projs);
	free(curr);
	curr=NULL;
}

void free_batch(batch *b){
	batch* curr=b;
	query* currq;
	int i;
	if(b==NULL){
		printf("This batch is empty.Exiting.\n");
		exit(-1);
	}
	for(i=0;i<curr->num_queries;i++){
		currq=curr->q_arr[i];
		free_query(currq);
	}
	free(curr->q_arr);
	curr->q_arr=NULL;
	free(curr);
	curr=NULL;
}