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
void store_pred(char* pred_str, pred* p){
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
	else { //an einai anapoda grammeno to fitro
		p->val=(uint64_t)atoi(token);
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
		p->val=(uint64_t)atoi(token);
		p->isFilter=1;
	}

	//twra tha apothikefsw ton operator tou kathgorhmatos 1 gia >, 2 gia < kai 3 gia =
	i=0;
	while((pred_str[i]!='>')&&(pred_str[i]!='<')&&(pred_str[i]!='=')) i++;
	if(pred_str[i]=='>') p->op=1;
	else if(pred_str[i]=='<') p->op=2;
	else p->op=3;

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
	pred *temp, *prev, *curr;
	curr=head;
	prev=curr;

	//Vazw ta selfjoins sthn arxh ths listas
	while(curr!=NULL){
		if((curr->isSelfjoin)&&(curr!=head)){
			prev->next=curr->next;
			temp=head;
			head=curr;
			head->next=temp;
		} 
		prev=curr;
		curr=curr->next;
	}
	q->preds=head;
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
}

void print_query(query q){
	int i,j;
	printf("The query uses the relations: \n");
	for(i=0;i<q.num_rels;i++)	printf("%d  ",q.rels[i]);

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
		if (curr->isFilter) printf("%ld", curr->val);
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
	pred *curr, *head, *prev;

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

	q->preds=malloc(sizeof(pred));
	if (q->preds == NULL) {
	    fprintf(stderr, "Malloc failed \n"); 
	    exit(-1);
	}
	i=0;
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
	head=q->preds;
	curr=head;
	prev=curr;
	for(i=0;i<num_preds;i++){
		//printf("%s\n",predicates[i]);
		if(i==0){
			store_pred(predicates[i],curr);
			prev=curr;
			curr=curr->next;
		} 
		else{
			curr=malloc(sizeof(pred));
			if (curr== NULL) {
			    fprintf(stderr, "Malloc failed \n"); 
			    exit(-1);
			}
			store_pred(predicates[i],curr);
			prev->next=curr;
			prev=curr;
			curr=curr->next;
		} 

	}
	q->preds=head;

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
			i++;
		}
		else{
			//printf("				The batch is over!\n\n\n");
			break;
		}		
	}
	return b;
}

void execute_workload(char* filename, int num_loadedrels, infoNode* infoMap){
	int i=0, num_batches;
	char buff[400];
	batch* b=NULL;
	long int offset=0, prev_offset=0;
	int numquery=0;

	FILE* fp=fopen(filename,"r"); //anoigei to arxeio eperwthsewn
	num_batches=count_batches(fp); //kai metraei posa batches eperwthsewn exei
	printf("This file has %d batches of queries.\n",num_batches );
	rewind(fp);
	i=0;
	for(i=0;i<num_batches;i++){
		b=store_batch(fp,&offset,&prev_offset,buff,400,b);
		//print_batch(b);
		execute_batch(b, num_loadedrels, infoMap, &numquery);		
		prev_offset=offset;
		free_batch(b);
		//exit(0);
	}
	fclose(fp);
	printf("Workload %s is done.\n", filename);
}

//ektelei mia omada eperwthsewn
void execute_batch(batch* b, int num_loadedrels, infoNode* infoMap ,int* numquery){
	int i;
	int count=*numquery;
	for(i=0;i<b->num_queries;i++){
		interm_node* interm=NULL;
		joinHistory* joinHist=NULL;
		printf("**********QUERY %d *************\n",count );
		interm=execute_query(interm, &joinHist, b->q_arr[i], infoMap, num_loadedrels);
		count++;
		//print_joinHist(joinHist, num_loadedrels);
	}
	*numquery=count;
}

//ektelei ena kathgorhma enos query
interm_node* execute_pred(interm_node* interm, joinHistory** joinHist,pred* p,int* rels, int num_loadedrels, infoNode* infoMap){
	int i=0,j=0,index;

	if(p->isFilter){
		int col,rel,indexOfrel;
		char cols[3]; //den pistevw na exei parapanw apo 999 sthles
		index=p->cols[i];
		rel=rels[index];
		i++;
		while(p->cols[i]!=-1){
			cols[j]=p->cols[i] +'0';
			j++;
			i++;
		}
		cols[j]='\0';
		sscanf(cols, "%d", &col);

		indexOfrel=index;

		//ektelw to filtro
		interm=filter(interm, p->op, infoMap, rel, indexOfrel, col, p->val, num_loadedrels);
	}
	else if(p->isSelfjoin){
		int col1,col2,rel,indexOfrel;
		char cols[3]; //den pistevw na exei parapanw apo 999 sthles
		index=p->cols[i];
		rel=rels[index];
		i++;
		while(p->cols[i]!=-1){
			cols[j]=p->cols[i] +'0';
			j++;
			i++;
		}
		cols[j]='\0';
		sscanf(cols, "%d", &col1);
		i+=2;//afou einai to idio relation prospernaw kai to deutero relation
		j=0;
		while(p->cols[i]!=-1){
			cols[j]=p->cols[i] +'0';
			j++;
			i++;
		}
		cols[j]='\0';
		sscanf(cols, "%d", &col2);

		indexOfrel=index;
		//ektelw to filtro
		interm=self_join(interm, infoMap, rel, indexOfrel, col1, col2, num_loadedrels);

	}
	else{
		int col1,col2,rel1,rel2,indexOfrel1,indexOfrel2;
		char cols1[3],cols2[3]; //den pistevw na exei parapanw apo 999 sthles
		index=p->cols[i];
		rel1=rels[index];
		indexOfrel1=index;
		i++;
		while(p->cols[i]!=-1){
			cols1[j]=p->cols[i] +'0';
			j++;
			i++;
		}
		cols1[j]='\0';
		sscanf(cols1, "%d", &col1);
		i++;
		j=0;
		index=p->cols[i];
		rel2=rels[index];
		indexOfrel2=index;

		i++;
		while(p->cols[i]!=-1){
			cols2[j]=p->cols[i] +'0';
			j++;
			i++;
		}
		cols2[j]='\0';
		sscanf(cols2, "%d", &col2);

		//ektelw to join
		interm=join2(interm, infoMap, joinHist, rel1, indexOfrel1, rel2, indexOfrel2, col1, col2, num_loadedrels);
		//print_joinHist(*joinHist, num_loadedrels);
	}
	return interm;		
}

//ektelei wna query
interm_node* execute_query(interm_node* interm, joinHistory** joinHist, query* q, infoNode* InfoMap, int num_loadedrels){
	int i;
	pred* curr;
	
	for(i=0;i<q->num_rels;i++){
		if(q->rels[i]>num_loadedrels-1){
			//fprintf(stderr, "The query has relations not included in the info map.Returning NULL\n");
			return NULL;
		}
	}
	curr=q->preds;
	while(curr!=NULL){
		interm=execute_pred(interm, joinHist, curr, q->rels, num_loadedrels, InfoMap);
		curr=curr->next;
	}
	proj_sums(interm,q, InfoMap);
	
	return interm;
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

	while((currp!=NULL)){
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

void proj_sums(interm_node* interm, query* q, infoNode* infoMap){
  uint64_t sum;
  int i,index=0,j=0,rel,rel_ind,col;
  printf("---SUMS---\n");
  while(j<q->num_projs){
  	sum=0;
    rel_ind=q->projs[index];
    rel=q->rels[rel_ind];
    col=q->projs[index+1];
    index+=3; //prospernaw kai to -1 sto telos tou projection
    for(i=0; i<interm->numOfrows[rel_ind]; i++ ){
	    sum+=return_value( infoMap, rel ,col, interm->rowIds[rel_ind][i]);
	}
	printf("-%ld\n",sum);
	j++;
  }
  printf("----------\n");
}
