#include "query.h"

int factorial(int n){
  return (n == 1 || n == 0) ? 1 : factorial(n - 1) * n;
}

//edw einai oles oi synarthseis gia to hash twn zeugariwn apo relations twn join 
int create_comb(int rel1, int rel2){
	int combination=0;

	if(rel1>rel2){
		combination+=rel1*10;
		combination+=rel2;
	}  
	else{
		combination+=rel2*10;
		combination+=rel1;
	}  
	//printf("------%d & %d = %d ---------\n",rel1, rel2, combination);
	return combination;

}
int relCombHash(int rel1, int rel2, int* relCombs,int numOfcombs){
	int i,combination;
	combination=create_comb(rel1,rel2);
   	
   	//hashcode= combination & ((1<<numOfrels)-1);
   	for(i=0;i<numOfcombs;i++){
   		if(relCombs[i]==combination) return i;
   	}
}

joinHash* create_joinHash(int numOfrels, pred* head){
	int i,j,k,index,rel1,rel2,combination,hash_index;
	pred* buckStart;
	pred* curr=head;
	pred* join,*curr_join;
	//twra tha vrw apo pou ksekinane ta join tou predicate array , diladi tha prospaerasw join kai selfjoin
	while(curr!=NULL){
		if((curr->isFilter)||(curr->isSelfjoin)) curr=curr->next;
		else break;
	}
	if(curr==NULL){
		fprintf(stderr, "No joins to do here!\n");
		return NULL;
	}
	//ftiaxnw th domh
	joinHash* jh=(joinHash*)malloc(sizeof(joinHash));
	if (jh== NULL) {
	    fprintf(stderr, "Malloc failed \n"); 
	    exit(-1);
	}
	jh->numOfrels=numOfrels;
	jh->numOfcombs=(factorial(jh->numOfrels)/((factorial(jh->numOfrels-2))*2));//de tha xrisimopoihthoun ola einai apla gia tin arxikopoihsh tou hash

	jh->relCombs=(int*)malloc(jh->numOfcombs*sizeof(int));
	if (jh->relCombs== NULL) {
	    fprintf(stderr, "Malloc failed \n"); 
	    exit(-1);
	}
	k=0;
	for(i=0;i<jh->numOfrels;i++){
		j=i+1;
		while(j<jh->numOfrels){
			int combo=create_comb(i,j);
			//printf("================================== %d\n",combo);
			jh->relCombs[k]=create_comb(i,j);
			j++;
			k++;
		}
	}

	
	jh->buckCount=(int*)malloc(jh->numOfcombs*sizeof(int));
	if (jh->buckCount== NULL) {
	    fprintf(stderr, "Malloc failed \n"); 
	    exit(-1);
	}
	jh->bucketArr=(pred**)malloc(jh->numOfcombs*sizeof(pred*));
	if (jh->bucketArr== NULL) {
	    fprintf(stderr, "Malloc failed \n"); 
	    exit(-1);
	}

	for(i=0;i<jh->numOfcombs;i++){
		jh->bucketArr[i]=NULL;
		jh->buckCount[i]=0;
	} 

	i=0;
	int count=0;
	while(curr!=NULL){
		i=0;
		count++;
		rel1=curr->cols[i]; //krataei to index tou prwtou rel
		i++;
		while(curr->cols[i]!=-1){ //opws parapanw krataei to string tou column
			i++;
		}
		i++;
		rel2=curr->cols[i]; //krataei to index tou deuterou rel
		hash_index=relCombHash(rel1,rel2,jh->relCombs,jh->numOfcombs);
		if(jh->bucketArr[hash_index]==NULL){
			jh->bucketArr[hash_index]=(pred*)malloc(jh->numOfcombs*sizeof(pred));
			if (jh->bucketArr[hash_index]== NULL) {
			    fprintf(stderr, "Malloc failed \n"); 
			    exit(-1);
			}
		}

		jh->bucketArr[hash_index][jh->buckCount[hash_index]]=*curr;
		printf("\n");
		jh->buckCount[hash_index]++;
		curr=curr->next;
	}

	return jh;
}

void statusOfJoinHash(joinHash* jh){
	int i,j,k,m,hashcode;
	for(i=0;i<jh->numOfrels;i++){
		j=i+1;
		while(j<jh->numOfrels){
			printf("--------Combinations %d%d or %d%d :\n",i,j,j,i);
			printf("--------Joins :\n");

			hashcode=relCombHash(i,j,jh->relCombs,jh->numOfcombs);;
			pred* curr=jh->bucketArr[hashcode];
			if(curr==NULL) printf("No joins available for this combination of relations.\n");
			for(k=0;k<jh->buckCount[hashcode];k++){
				printf("%d.",curr[k].cols[0]);
				m=1;
				while(curr[k].cols[m]!=-1){
					printf("%d",curr[k].cols[m]);
					m++;
				}
				m++;
				printf(" "); 
				printf("= ");
				printf("%d.",curr[k].cols[m]);
				m++;
				while(curr[k].cols[m]!=-1){
					printf("%d",curr[k].cols[m]);
					m++;
				}				
				printf("\n");
			}
			j++;
		}
	}
}

void free_joinHash(joinHash* jh){
	int numOfrels,i;
	for(i=0;i<jh->numOfcombs;i++){
		free(jh->bucketArr[i]);
	}
	free(jh->bucketArr);
	free(jh->buckCount);
	free(jh->relCombs);
	free(jh);
	jh=NULL;
}



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
	temp=(char*)malloc(((strlen(preds))+1)*sizeof(char));
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
	temp=(char*)malloc(((strlen(rels))+1)*sizeof(char));
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

//ftixnei lista me ta strings twn queries pou dexetai apo to stdin, gia na dwthoun stin sinexeia sthn synarthsh execute
query_list* add_quNode(query_list** quHead, query_list* quList, char* buff){
  query_list* curr=quList;

  if(*quHead==NULL){ //ean proketai gia to prwto query pou dinetai desmevei th lista
    curr=*quHead;
    curr=(query_list*)malloc(sizeof(query_list));
    if(curr==NULL){ fprintf(stderr, "Malloc failed \n"); return NULL;}
    strcpy(curr->query,buff);
    curr->next=NULL;

    *quHead=curr;
    return curr;
  }

  if(curr->next==NULL){ //kai desmevei xwro gia to query ekei pou prepei kai apothikevei to string
    curr->next=(query_list*)malloc(sizeof(query_list));
    if(curr->next==NULL){ fprintf(stderr, "Malloc failed \n"); return NULL;}
    strcpy(curr->next->query,buff);
    curr->next->next=NULL;
  }
  
  return curr->next;
}

//ektypwnei thn trexousa katastash ths listas me ta string twn dosmenwn ews twra queries
void print_quList(query_list* quList){
  query_list* curr=quList;
  while(curr!=NULL){
    printf("%s\n",curr->query );
    curr=curr->next;
  }
}

//apodesmevei tin mnhmh pou exei desmeftei gia thn lista twn strings twn queries
void free_quList(query_list* quList){
	query_list* curr=quList, *temp;
	while(curr!=NULL){
		temp=curr;
		curr=curr->next;
		free(temp);
	}
}

//apothikevei ena pred sth thesh pou prepei sth lista predicates tou query
void store_pred(char* pred_str, pred* p){
	int i,dots=0, index,rel1,rel2;
	char *token, *predicate, delim[]="><=";
	
	//sthn arxh thwrw oti to sigekrimeno kathgorhma den einai oute filtro oute selfJoin gia na ta allaksw sth poreia
	p->isFilter=0;
	p->isSelfjoin=0;

	//antigrafw to dothen string tou kathgorhmatos se ena allo gia na min berdeftei h strtok
	predicate=(char*)malloc(((strlen(pred_str))+1)*sizeof(char));
	if (predicate== NULL) {
	    fprintf(stderr, "Malloc failed \n"); 
	    exit(-1);
	}
	strcpy(predicate, pred_str);

	//desmevw xwro gia ta columns estw oti to pred einai 1.22=3.2 tote exw strlen-2 ,diladi afairw ta dots .ta cols diaxwrizontai me -1
	p->cols=(int*)malloc((strlen(predicate)-1)*sizeof(int)); //enw ta dots einai 2 ,  iparxei kai -1 pou apothikevetai sto telos opote desmevw strlen-1 
	if (p->cols== NULL) {
	    fprintf(stderr, "Malloc failed \n"); 
	    exit(-1);
	}
	//kanw tokenize to kathgorima
	token= strtok(predicate,delim);  //pairnw to prwto tmhma tou kathgorhmatos pou einai sxedon panta mia sthlh enos relation
	if(token==NULL){
		fprintf(stderr,"Not apropriate format for a predicate.\n");
		exit(-1);
	}
	for(i=0;i<strlen(token);i++){ //metraw posa dots iparxoun sto tmima tou kathgorhmatos, gia na dw ena einai col h value
		if(token[i]=='.') dots++;
	}
	if(dots==1){ //exw column, ara apothikevw ta columns tou predicate
		index=0;
		rel1=token[0] - '0';

		//apothikevw to column
		for(i=0;i<strlen(token);i++){
			if(token[i]!='.'){
				p->cols[index]=token[i] - '0';
				index++;
			}
		}
		p->cols[index]=-1; //diaxwrhstiko metaksi twn columns valw panta ena -1
		index++;
	}
	else { //ean den exw dots einai value, ara an einai anapoda grammeno to fitro diladi exw px 300>2.1. 
		p->val=(uint64_t)atoi(token); //apothikevw to value
		p->isFilter=1;	//orizw to kathgorhma ws filtro
	}

	//pairnw to deutero tmhma tou kathgorhmatos
	dots=0;
	token= strtok(NULL,delim); 
	if(token==NULL){
		fprintf(stderr,"Not apropriate format for a predicate.\n");
		exit(-1);
	}
	for(i=0;i<strlen(token);i++){ //metraw pali ta dots gia na dw ean prokeitai gia column h value
		if(token[i]=='.') dots++;
	}
	if(dots==1){ //ean exw ena dot paei na pei pws einai column
		rel2=token[0] - '0';
		//twra tha elegksw an to kathgorima einai selfjoin
		if(rel1==rel2) p->isSelfjoin=1;

		//apothikevw to column
		for(i=0;i<strlen(token);i++){
			if(token[i]!='.'){
				p->cols[index]=token[i] - '0';
				index++;
			}
		}
		p->cols[index]=-1; //diaxwrhstiko twn columns einai ena -1
	}
	else { //alliws prokeitai gia filtro
		p->val=(uint64_t)atoi(token);
		p->isFilter=1;
	}

	//twra tha apothikefsw ton operator tou kathgorhmatos 1 gia >, 2 gia < kai 3 gia =
	i=0;
	while((pred_str[i]!='>')&&(pred_str[i]!='<')&&(pred_str[i]!='=')) i++;
	if(pred_str[i]=='>') p->op=1;
	else if(pred_str[i]=='<') p->op=2;
	else p->op=3;

	p->next=NULL; //kanw null ton epomeno komvo sth lista twn kathgorhmatwn tou query

	free(predicate); //apodesmevw ton xwro gia to copycat string tou kathgorhmatos
}

//apothikevei ta projections tou query
void store_proj(char* proj_str, query* q){
	int i,dots=0, index;
	char *token, *projections;

	int* proj=q->projs;
	
	//ftiaxnw ena antigrafo tou string twn provolwn gia na mi berdeftei sth synexeia h strtok
	projections=(char*)malloc(((strlen(proj_str))+1)*sizeof(char));
	if (projections== NULL) {
	    fprintf(stderr, "Malloc failed \n"); 
	    exit(-1);
	}
	strcpy(projections, proj_str);

	//metraw posa einai ta projections gia na desmefsw xwro
	for(i=0;i<strlen(projections);i++){
		if(projections[i]=='.') dots++;
	}
	q->num_projs=dots; //osa einai ta dots toses einai kai oi provoles pou zitaei to query

	//desmevw ton apaitoumeno xwro gia ta projections
	proj=(int*)malloc((strlen(projections)-dots+1)*sizeof(int));
	if (proj== NULL) {
	    fprintf(stderr, "Malloc failed \n"); 
	    exit(-1);
	}

	token= strtok(projections," \n");//pairnw to prwto projection
	if(token==NULL){
		fprintf(stderr,"Not apropriate format for a predicate.\n");
		exit(-1);
	}
	index=0;
	while(token!=NULL){ //mexri na teleiwsoun oi provoles
		for(i=0;i<strlen(token);i++){ //pali, opws kai sta columns twn predicates, apothikevei ta columns xwris ta dots me diaxwristiko to -1
			if(token[i]!='.'){
				proj[index]=token[i] - '0';
				index++;
			}
		}
		proj[index]=-1;
		index++;
		token= strtok(NULL," \n");
	}

	q->projs=proj;
	
	free(projections); //apodesmevei ton xwro tou copycat string		
}

//taksinomei ti lista twn kathgorhmatwn me auto to tropo |filters|selfjoins|rest of predicates...
void reorder_preds(query* q){
	pred* head=q->preds;
	pred *temp, *prev, *curr;
	curr=head;
	prev=curr;

	//Vazw ta selfjoins sthn arxh ths listas gia na einai meta eukolo na valw ta filtra pali stin arxh kai na mi psaxnw thn thesh 
	while(curr!=NULL){
		if((curr->isSelfjoin)&&(curr!=head)){ //psanei ola ta predicates kai an vrei self join to vazei thn arxh ths listas
			prev->next=curr->next;
			temp=head;
			head=curr;
			head->next=temp;
		} 
		prev=curr;
		curr=curr->next;
	}
	q->preds=head; //pernei pali thn lista apo thn kefalh ths
	curr=head;
	prev=curr;
	//Vazw ta filtra sthn arxh ths listas
	while(curr!=NULL){ //psaxnei ola ta predicates kai an vrei filtro to vazei sthn arxh ths listas
		if((curr->isFilter)&&(curr!=head)){
			prev->next=curr->next;
			temp=head;
			head=curr;
			head->next=temp;
		} 
		prev=curr;
		curr=curr->next;
	}

	q->preds=head; //dinw sthn lista tou query thn arxh ths listas pou exw anadiorganwsei
}

//ektypwnei eola ta dedomena pou einai apothikevmena se mia domh query
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

//apokwdikopoiei to dosmeno string query kai apothikevei ta tmimata tou sti domh query
query* store_query(char* qstr, query* q){
	int i=0,num_preds,num_rels;
	char *token, *rels, *preds, *projs;
	pred *curr, *head, *prev;

	//desmevei xwro gia thn domh query sthn opoia tha apothikeftei to dosmeno query
	q=(query*)malloc(sizeof(query));
	if (q== NULL) {
	    fprintf(stderr, "Malloc failed \n"); 
	    exit(-1);
	}


	//spaw to qstr sta tria tmimata kai apothikevw ta tmimata se 3 diaforetika strings gia na kalesw epeita tis diafores store
	// gia ta RELATIONS
	token= strtok(qstr,"|"); 
	if(token==NULL){
		fprintf(stderr,"Not apropriate format for a query.\n");
		exit(-1);
	}
	rels=(char*)malloc((strlen(token)+1)*sizeof(char));
	if (rels== NULL) {
	    fprintf(stderr, "Malloc failed \n"); 
	    exit(-1);
	}
	strcpy(rels, token);

	// gia ta PREDICATES
	token= strtok(NULL,"|");
	if(token==NULL){
		fprintf(stderr,"Not apropriate format for a query.\n");
		exit(-1);
	}
	preds=(char*)malloc((strlen(token)+1)*sizeof(char));
	if (preds== NULL) {
	    fprintf(stderr, "Malloc failed \n"); 
	    exit(-1);
	}
	strcpy(preds, token);

	//gia ta PROJECTIONS
	token= strtok(NULL,"|");
	if(token==NULL){
		fprintf(stderr,"Not apropriate format for a query.\n");
		exit(-1);
	}
	projs=(char*)malloc((strlen(token)+1)*sizeof(char));
	if (projs== NULL) {
	    fprintf(stderr, "Malloc failed \n"); 
	    exit(-1);
	}
	strcpy(projs, token);


	//Apothikevw sth domy query ta dedomena apo to string tou query
	//RELATIONS
	num_rels=count_rels(rels);
	q->rels=(int*)malloc(num_rels*sizeof(int)); //desmevw xwro gia ta relations tou query
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
	num_preds=count_preds(preds);

	q->preds=(pred*)malloc(sizeof(pred)); //desmevw xwro gia thn lista twn kathgorhmatwn ths domhs query
	if (q->preds == NULL) {
	    fprintf(stderr, "Malloc failed \n"); 
	    exit(-1);
	}
	i=0;
	//spaw ola ta kathgorhmata kai ta kratw se ksexwrista string gia na mi berdeftei h strtok
	char *predicates[num_preds];
	token= strtok(preds,"&"); //painrw to prwto kathgorhma
	if(token==NULL){
		fprintf(stderr,"Not apropriate format for a predicate.\n");
		exit(-1);
	}
	predicates[0]=(char*)malloc((strlen(token)+1)*sizeof(char)); //desmevw xwro gia to prwto kathgorhma string
	if (predicates[0] == NULL) {
	    fprintf(stderr, "Malloc failed \n"); 
	    exit(-1);
	}

	strcpy(predicates[0],token);//to apothikevw

	//apothikevw ola ta ksexwrista string kathgorhmata
	for(i=1;i<num_preds;i++){
		token= strtok(NULL,"&"); //pairnw ena ena ta kathgorhmata
		if(token==NULL){
			fprintf(stderr,"Not apropriate format for a predicate.\n");
			exit(-1);
		}
		predicates[i]=(char*)malloc((strlen(token)+1)*sizeof(char));
		if (predicates[i] == NULL) {
		    fprintf(stderr, "Malloc failed \n"); 
		    exit(-1);
		}

		strcpy(predicates[i],token); //ta apothikevw

	}
	head=q->preds;
	curr=head;
	prev=curr;
	for(i=0;i<num_preds;i++){ //apokwdikopoiw kai apothikevw kathe kathgorhma sthn lista ths domhs query
		if(i==0){
			store_pred(predicates[i],curr); //apothikevw to prwto apokwdikopoihmeno kathgorhma
			prev=curr;
			curr=curr->next;
		} 
		else{
			curr=(pred*)malloc(sizeof(pred)); //desmevw xwro gia to kathgorhma sth lista
			if (curr== NULL) {
			    fprintf(stderr, "Malloc failed \n"); 
			    exit(-1);
			}
			store_pred(predicates[i],curr); //apothikevw sti lista tou query to apokwdikopoihmeno kathgorhma
			prev->next=curr;
			prev=curr;
			curr=curr->next;
		} 

	}
	q->preds=head; //dinw sth lista tou query thn lista pou eftiaksa me ta kathgorhmata

	//PROJECTIONS
	store_proj(projs, q); //apokwdikopooiw to string twn projections kai ta apothikevw sth domh query	


	//apodesmevw ton xwro pou pianoun ta spasmena strings tou string tou query
	free(rels);
	free(preds);
	free(projs);

	//apodesmevw ton xwro pou pianoun ta copycats twn kathgorhmatwn strings
	for(i=0;i<num_preds;i++){
		free(predicates[i]);
	}

	return q; //epistrefw thn domh query pou eftiaksa

}

//desmevei kai apothikevei ena batch me ti lista apo queries pou tou exoun dothei
batch* store_batch(query_list* quList, int num_queries, batch* b){
	int i=0;
	query_list* curr=quList; //h lista me ta string twn queries pou exoun dothei apo to stdin
	if(quList==NULL){
		fprintf(stderr, "There are no queries for this batch\n" );
		return NULL;
	}	
	if(b==NULL){ //desmevw xwro gia to batch pou tha dimiourghthei apo ta dosmena queries
		b=(batch*)malloc(sizeof(batch));
		if (b== NULL) {
	    	fprintf(stderr, "Malloc failed \n"); 
	    	exit(-1);
		}
	}

	////desmevw xwro gia ton array twn dosmenwn queries
	b->q_arr=(query**)malloc(num_queries*sizeof(query*));
	if (b->q_arr== NULL) {
    	fprintf(stderr, "Malloc failed \n"); 
    	exit(-1);
	}
	b->num_queries=num_queries; //orizw sto batch posa queries exei

	for(i=0;i<num_queries; i++ ){ //gia kathe ena apo ta string queries sti lista twn queries, apokwdikopoiw kai to apothuikevw sto batch
		b->q_arr[i]=store_query(curr->query,b->q_arr[i]);
		reorder_preds(b->q_arr[i]); //anadiorganwnw ta kathgorhmata tou query na einai |filtra|selfjoins|rest of queries
		curr=curr->next;
	}
	
	return b; //epistrefw to batch pou dimiourghsa
}

//ektelei to dosmeno workload apo to stdin
void execute_workload( int num_loadedrels,infoNode* infoMap){
	int s;
	char buff[500];
	batch* b=NULL;
	int numquery=0, numOfqueries=0; 
	query_list* quList=NULL;
	query_list* quCurr=quList;

	fgetc(stdin);

	//oso den dinetai input apo to stdin, diladi oso iparxoun queries sto workload
	while(1){
		fflush(stdout);//katharizw to stdout

		if(fgets(buff, 500, stdin)==NULL) break;//ean teleiwsei to workload epistrefei
		
		s=strlen(buff); //epeidh h fgets apothikevei sto string kai to \n kai de to theloume vazoume sth thesh tou to end of string me \0
		buff[s-1]='\0';
		if(strcmp(buff,"F")==0 || strcmp(buff,"f")==0){ //ean exei dothei to f h to F, einai to sima oti teleiwse to batch
			b=store_batch(quList, numOfqueries, b); //dimhourgei to batch me ta dosmena queries
			execute_batch(b, num_loadedrels, infoMap, &numquery); //ektelei to batch
			numOfqueries=0;

			free_quList(quList); //apodesmevei th lista twn dosmenwn query strings
			quList=NULL;
			quCurr=quList;
			free_batch(b); //apodesmevei ton xwro pou exei desmeftei gia to batch
			b=NULL;		
		}
		else{ //alliws prosthetei to dosmeno query stin lista pou tha dothei gia th dimiourgeia kai thn ektelesh tou batch
			numOfqueries+=1;
			quCurr=add_quNode(&quList, quCurr, buff);

		}	
	}
	fprintf(stderr, "Workload is done\n" );
}

//ektelei mia omada eperwthsewn pou apoteloun to batch
void execute_batch(batch* b, int num_loadedrels, infoNode* infoMap ,int* numquery){
	int i;
	int count=*numquery;
	//gia ola ta queries pou exei i lista pou dimiourgithike apo ta string tou stdin
	for(i=0;i<b->num_queries;i++){
		interm_node* interm=NULL;
		joinHistory* joinHist=NULL;
		fprintf(stderr,"**********QUERY %d *************\n",count );
		interm=execute_query(interm, &joinHist, b->q_arr[i], infoMap, num_loadedrels); //ektelei to kathe query
		count++;
	}
	*numquery=count;
}

//ektelei ena kathgorhma enos query
interm_node* execute_pred(interm_node* interm, joinHistory** joinHist,pred* p,int* rels, int num_loadedrels, infoNode* infoMap, stats** qu_stats){
	int i=0,j=0,index;
	stats* rel_stats=*qu_stats;
	stats* rel1_stats=*qu_stats;
	stats* rel2_stats=*qu_stats;

	if(p->isFilter){ //ean prokeitai gia filtro
		int col,rel,indexOfrel;
		char cols[3]; //thewrw ori den exei parapanw apo 999 columns
		index=p->cols[i]; //krataei to index tou relation
		rel=rels[index]; //krataei to pragmatiko relation
		i++;
		while(p->cols[i]!=-1){ //krataei to column ws string, giati stin periptwsh pou den einai monopsifios den borei na apothikeftei alliws
			cols[j]=p->cols[i] +'0';
			j++;
			i++;
		}
		cols[j]='\0'; //termatizei to string column
		sscanf(cols, "%d", &col); //metatrepei to string tou column se integer

		indexOfrel=index;

		//ektelw to filtro
		rel_stats=&(rel_stats[indexOfrel]);
		//printf("PRED index %d columns  %d \n",indexOfrel, rel_stats->columns );
		interm=filter(interm, p->op, infoMap, rel, indexOfrel, col, p->val, num_loadedrels, rel_stats);
		//print_stats( rel_stats, num_loadedrels);
	}
	else if(p->isSelfjoin){ //prokeitai gia selfjoin
		int col1,col2,rel,indexOfrel;
		char cols[3]; //thewrw oti den exei parapawn apo 999 columns
		index=p->cols[i]; //krataei to index tou relation
		rel=rels[index]; //krataei to pragmatiko relation
		i++;
		while(p->cols[i]!=-1){ //krataei se string to column, se periptwsh pou den einai monopsifio den ginetai alliws
			cols[j]=p->cols[i] +'0';
			j++;
			i++;
		}
		cols[j]='\0'; //termatizei to string tou column
		sscanf(cols, "%d", &col1); //metatrepei to column se integer
		i+=2;//afou einai to idio relation prospernaw kai to deutero relation
		j=0;
		while(p->cols[i]!=-1){ //krataei se string to column, se periptwsh pou den einai monopsifio den ginetai alliws
			cols[j]=p->cols[i] +'0';
			j++;
			i++;
		}
		cols[j]='\0'; //termatizei to string tou column 
		sscanf(cols, "%d", &col2); //to metatrepei se integer

		indexOfrel=index;
		//ektelw to selfjoin
		rel_stats=&(rel_stats[indexOfrel]);
		//printf("PRED index %d columns  %d \n",indexOfrel, rel_stats->columns );
		interm=self_join(interm, infoMap, rel, indexOfrel, col1, col2, num_loadedrels, rel_stats);
		//print_stats( rel_stats, num_loadedrels);

	}
	else{ //prokeitai gia aplo join
		int col1,col2,rel1,rel2,indexOfrel1,indexOfrel2;
		char cols1[3],cols2[3]; //den pistevw na exei parapanw apo 999 sthles
		index=p->cols[i]; //krataei to index tou prwtou rel
		rel1=rels[index]; //krataei to pragmatiko prwto rel
		indexOfrel1=index;
		i++;
		while(p->cols[i]!=-1){ //opws parapanw krataei to string tou column
			cols1[j]=p->cols[i] +'0';
			j++;
			i++;
		}
		cols1[j]='\0'; //termatizei to string
		sscanf(cols1, "%d", &col1); //to metatrepei se integer
		i++;
		j=0;
		index=p->cols[i]; //krataei to index tou deuterou rel
		rel2=rels[index]; //krataei to deutero rel
		indexOfrel2=index;

		i++;
		while(p->cols[i]!=-1){ //apothikevei to column
			cols2[j]=p->cols[i] +'0';
			j++;
			i++;
		}
		cols2[j]='\0';
		sscanf(cols2, "%d", &col2);

		//ektelw to join
		rel1_stats=&(rel1_stats[indexOfrel1]);
		rel2_stats=&(rel2_stats[indexOfrel2]);
		//printf("Join : %d.%d = %d.%d\n",rel1,col1,rel2,col2);
		//print_stats( rel_stats, num_loadedrels);
		update_joinStats(rel1_stats, rel2_stats, col1, col2);
		//print_stats( rel_stats, num_loadedrels);
		interm=join2(interm, infoMap, joinHist, rel1, indexOfrel1, rel2, indexOfrel2, col1, col2, num_loadedrels);
	}
	return interm; //epistrefw to intermediate		
}

void free_stats(stats* qu_stats, int numOfrels){
	int i;
	for(i=0; i<numOfrels; i++){
		free(qu_stats[i].u);
		free(qu_stats[i].l);
		free(qu_stats[i].f);
		free(qu_stats[i].d);
	}
	free(qu_stats);
}

//ektelei ena query tou batch
interm_node* execute_query(interm_node* interm, joinHistory** joinHist, query* q, infoNode* InfoMap, int num_loadedrels){
	int i,j,r,clmns;
	pred* curr;
	long long int* crossArr=NULL;
	stats* qu_stats=(stats*)malloc(num_loadedrels* sizeof(stats));
	for(i=0; i<q->num_rels; i++){
		r=q->rels[i];
		clmns=InfoMap[r].columns;
		qu_stats[i].columns=clmns;
		//printf("REl %d, i: %d with columns %d \n",r,i,clmns );

		qu_stats[i].u=(uint64_t*)malloc(clmns* sizeof(uint64_t));
		memcpy(qu_stats[i].u, InfoMap[r].u, clmns*sizeof(uint64_t));
		
		qu_stats[i].l=(uint64_t*)malloc(clmns* sizeof(uint64_t));
		memcpy(qu_stats[i].l, InfoMap[r].l, clmns*sizeof(uint64_t));
		
		qu_stats[i].d=(double*)malloc(clmns* sizeof(double));
		memcpy(qu_stats[i].d, InfoMap[r].d, clmns*sizeof(double));
		
		qu_stats[i].f=(double*)malloc(clmns* sizeof(double));
		memcpy(qu_stats[i].f, InfoMap[r].f, clmns*sizeof(double));
		/*for(j=0; j<clmns; j++){
			printf("\tCOLUMN: %d\n",j );
			printf("\t\t--Info: u:%ld, l:%ld, f:%lf, d:%lf \n",InfoMap[r].u[j], InfoMap[r].l[j],InfoMap[r].f[j], InfoMap[r].d[j]);
			printf("\t\tStats: u:%ld, l:%ld, f:%lf, d:%lf --\n",qu_stats[i].u[j],qu_stats[i].l[j],qu_stats[i].f[j],qu_stats[i].d[j] );
		}*/

	}
	//print_stats( qu_stats, q->num_rels);
	//arxika elegxw an to query einai valid
	for(i=0;i<q->num_rels;i++){
		if(q->rels[i]>num_loadedrels-1){
			//fprintf(stderr, "The query has relations not included in the info map.Returning NULL\n");
			return NULL;
		}
	}
	joinHash* jh=create_joinHash(q->num_rels, q->preds);
	statusOfJoinHash(jh);

	curr=q->preds;
	while(curr!=NULL){ //ektelw ola ta kathgorhmata pou exei to query
		interm=execute_pred(interm, joinHist, curr, q->rels, q->num_rels, InfoMap, &qu_stats);

		if(interm==NULL)break;
		curr=curr->next;
	}

	//Kalw thn cross_nodes h opoia epistrefei null ean den xreiazetai na ginei cross metaksi twn relations pou iparxoun sto intermediate
	if(interm!=NULL) crossArr=cross_nodes(interm, q->rels, InfoMap, joinHist, q->num_rels);
	if(crossArr==NULL) {
		proj_sums(interm,q, InfoMap); //an de xreiastike cross ftiaxnw ta athroismata twn provolwn kanonika
	}
	else { //alliws ftiaxnw ta athroismata siberilamvanontas panta ta duplicates tou cross
		proj_sumsAfterCross(crossArr, interm, q, InfoMap);
		free(crossArr); //apodesmevw ton xwro pou pianei o array twn multiplyin values tou cross
	}

	free_interm( interm); //apodesmevw to intermediate
	free_joinistory( *joinHist); //apodesmevw to history twn joins
	free_stats( qu_stats,q->num_rels);
	free_joinHash(jh);
	return interm;
}

//ektypwnei to batch
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

//apodesmevei thn mnhmh pou exei desmevtei gia ola osa xreiazetai mia domh query
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

//apodesmevei ton xwro pou exei desmevtei gia ena batch
void free_batch(batch *b){
	batch* curr=b;
	query* currq;
	int i;
	if(b==NULL){
		fprintf(stderr,"This batch is empty.Exiting.\n");
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

//ipologizei ta athroismata twn provolwn sth periptwsh pou den exei xreiastei na ginei cross sto intermediate
void proj_sums(interm_node* interm, query* q, infoNode* infoMap){
	uint64_t sum;
	int i,index=0,j=0,rel,rel_ind,col;

	//ipologizei to sum gia kathe mia apo tis zitoumenes provoles
	while(j<q->num_projs){
		sum=0;
		rel_ind=q->projs[index];
		rel=q->rels[rel_ind];
		col=q->projs[index+1];
		index+=3; //prospernaw kai to -1 sto telos tou projection

		//upologizw kai ektypwnw ena ena athroismata ths kathe provolhs
		for(i=0; i<interm->numOfrows[rel_ind]; i++ ){
			sum+=return_value( infoMap, rel ,col, interm->rowIds[rel_ind][i]);
		}
		//ektypwnw ta sums
		if(j==q->num_projs-1){
			if(sum==0) printf("NULL\n");
			else printf("%ld\n",sum);
		}
		else {
			if(sum==0) printf("NULL ");
			else printf("%ld ",sum);
		}
		j++;
	}
}

//ipologizei ta sums otan exei xreiastei na ginei cross sta relations pou iparxoun sto intermediate
void proj_sumsAfterCross(long long int* toMul, interm_node* interm, query* q, infoNode* infoMap){
	long long int sum;
	uint64_t* ptr;
	int i,index=0,j=0,rel,rel_ind,col;
	
	//gia kathe mia apo tis provoles
	while(j<q->num_projs){
		sum=0;
		rel_ind=q->projs[index];
		rel=q->rels[rel_ind];
		col=q->projs[index+1];
		index+=3; //prospernaw kai to -1 sto telos tou projection

		//ean  to relation exei xrisimopoihthei mono gia to cross kai se kamia praksi, tote ipologizw ta athroismata apo to inFomap
		if(interm->rowIds[rel_ind]==NULL){
			ptr=(uint64_t*)infoMap[rel].addr[col];
			for(i=0;i<interm->numOfrows[rel_ind];i++){
				sum+=*ptr;
				ptr++;
			}
		}
		else{ //alliws ipologizei ta athroismata kanonika apo ta valuws tou intermediate
			for(i=0; i<interm->numOfrows[rel_ind]; i++ ){
			    sum+=return_value( infoMap, rel ,col, interm->rowIds[rel_ind][i]);
			}
		}
		
		sum=sum*toMul[rel_ind]; //ipologizw sto sum tous pollaplasiasmous tou exei ipostei to relation logw tou cross
		
		printf("%lld ",sum);
		j++;
	}
	printf("\n");

}
