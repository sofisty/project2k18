#include "hash1.h"
#include "hash2.h"


int hash_func(int value, int n){ //hash function gia thn tmhmatopoish , dimiourgia evretiriwn sta R kai S
	int l=1;
	for(int i =0; i<n; i++) l*=2;
	return (value%l);
}

hist_node*  update_hist(hist_node* head, int hash_val, int* total_buckets, int* data){ // dimiourgei h enhmerwnei to dothen istogramma
	hist_node* temp;
	int flag=0;
	if(head==NULL){ //dimiourgei to keno dothen istogramma
		head = malloc(sizeof(hist_node));
		if (head == NULL) {
	    fprintf(stderr, "Malloc failed \n"); return NULL;
		}

		head->hash_val = hash_val; //arxikopoiw to istogramma me thn pdothisa hashvalue
		head->count=1;
		head->next=NULL;
		*total_buckets=1;
		*data=1;
		return head;
	}	
	hist_node * current = head; 
	if(current->hash_val==hash_val){ current->count+=1; *data+=1; return head; }//An einai i timh mikroterh h ish tou head
	if(current->hash_val>hash_val){// an einai mikroterh thn prosthetw prin to head 
		temp=head;
		head = malloc(sizeof(hist_node));
		if (head == NULL) {
	    fprintf(stderr, "Malloc failed \n"); return NULL;
		}

		head->hash_val = hash_val;
		head->count=1;
		head->next=temp;
		*total_buckets+=1;
		*data +=1;
		return head;
	}

    while (current->next != NULL) { //an den einai mikroterh ths prwths timhs psaxnw sthn upoloiph lista thn thesh ths, me vash thn auksousa seira twn timwn
    	if(current->next->hash_val==hash_val) { current->next->count+=1; *data+=1; return head; } //an uparxei
    	if(current->next->hash_val<hash_val){  //an einai megaluterh h hash timh apo thn hdh apothhkeumenh proxwraw thn lista mexri na vrw kapoia h na teleiwsei h lista
    		current = current->next;
       		
    	}
    	else{ flag=1; break;} //Vrhka thn prwth apothhkeumenh timh megaluterh ths hash_val, wste na thn prosthesw prin apo authn

        
    }
    if(flag==1){ 
    	temp=current->next; //h epomenh timh auths pou tha apothhkeusw
    }
    else{ //den uparxei epomenh timh
    	temp=NULL;
    }
    
    current->next = malloc(sizeof(hist_node)); //dimiourgw sti thesi pou vrika ti nea eggrafi tou istogrammatos
    if (current->next == NULL) {
	    fprintf(stderr, "Malloc failed \n"); return NULL;
	}
    current->next->hash_val = hash_val;
    current->next->count=1;
    current->next->next = temp;
    *total_buckets+=1;
    *data+=1;
    

    return head;//epistrefw tin kefalh tou istogrammatos 

}

void print_hist(hist_node * head) { //typwnei to istogramma sth parousa katastash sou
    hist_node * current = head;

    while (current != NULL) {
        printf("Hash value: %d includes %d items\n", current->hash_val, current->count);
        current = current->next;
    }
}

void free_hist(hist_node* head){ //kanei free thn memory pou exei desmeutei gia to istogramma
	hist_node* temp=head;
	hist_node* curr=head;

	while(curr->next!=NULL){
		temp=curr;
		curr=curr->next;
		free(temp);
		temp=NULL;
	}
	free(curr);
	curr=NULL;
	
}


psum_node* create_psumlist(psum_node* psum_head, hist_node* hist_head ){ //dhmiourgei thn lista me ta psum
	 int offset=0;
	 psum_node* curr;

	 hist_node* hist_current=hist_head;
	 while(hist_current!=NULL){ // oso iparxoun eggrafes so istogramma pou exei dothei
	 	if(psum_head==NULL){ // an to psum list einai keno to dimiourgei eksarxeis
			psum_head=malloc(sizeof(psum_node));
			if (psum_head == NULL) { fprintf(stderr, "Malloc failed \n"); return NULL;}
			psum_head->hash_val=hist_current->hash_val; //kai to arxikopoiei me tin dotheia hash value
			psum_head->offset=offset;
			psum_head->curr_off=offset; //apothikevei to offset apo to opoio ksekinaei sto relation to tmima me authn thn hash value
			psum_head->next=NULL;
			curr=psum_head;

		}
		else{
			if(curr->next==NULL){ //alliws prosthetw sto telos tou psum_list tin current eggrafi tou istogrammatos
				curr->next=malloc(sizeof(psum_node));
				if (curr == NULL) {fprintf(stderr, "Malloc failed \n"); return NULL;}
				curr->next->hash_val=hist_current->hash_val;
				curr->next->offset=offset;
				curr->next->curr_off=offset;
				curr->next->next=NULL;
				curr=curr->next;
			}
			//else lathos 
			
		}
		offset+=hist_current->count;//proxwraei to offset oses einai oi eggrafes pou exoun hash value oso to eksetazomeno (to pairnei apo to count tou hist)
	 	hist_current=hist_current->next;
	 }


   return psum_head; //epistrefei tin kefalh ths listas

}


void print_psum(psum_node * head) { //ektypwnei tin psum lista
    psum_node * current = head;

    while (current != NULL) {
        printf("Hash value: %d starts from %d \n", current->hash_val, current->offset);
        //printf("\tCurrent offset %d\n", current->curr_off);
        current = current->next;
    }
}


void free_psum(psum_node* head){ //apodesmevei thn mnh mou eixame dwsei sthn psum lista
	psum_node* temp=head;
	psum_node* curr=head;

	while(curr->next!=NULL){
		temp=curr;
		curr=curr->next;
		free(temp);
		temp=NULL;
	}
	free(curr);
	curr=NULL;

	
}


int search_Psum(psum_node* head, int key, int n ){ //psaxnei tin psum lista gia na vrei to offset sto opoio tha prostethei to key
	psum_node* curr=head;
	int hash_val;
	int offset;
	
	hash_val=hash_func(key, n);//vriskei thn hashvalue tou dothentos key

	while(curr!=NULL){ //oso exei eggrafes h psum
		if(curr->hash_val==hash_val){ //psaxnw to sygekrimeno hash value
			offset=curr->curr_off;
			curr->curr_off++;
			return offset;//epistrefei to offset
		}
		curr=curr->next;
	}
	return 1;
}


relation* reorder_R(psum_node* phead, relation* R, relation* R_new, int n  ){ //kanei reorder na relation me vasi thn psum list
	int i, size=R->num_tuples;
	int  offset, key, payload;
	R_new->num_tuples=size;
	R_new->tuples=malloc(size* sizeof(tuple));
	if(R_new->tuples==NULL){ fprintf(stderr, "Malloc failed \n"); return NULL;}

	for(i=0; i<size; i++){ //ftiaxnei to R_new pou tha epistrafei me thn seira etsi wste na armozei h tmhmatopoihsh tou
						   // me auth thn psum kai tou isogrammatos
		key=R->tuples[i].key;
		payload=R->tuples[i].payload;
		offset=search_Psum( phead, key, n );
		//print_psum( phead);
		R_new->tuples[offset].key=key;
		R_new->tuples[offset].payload=payload;
	}
	return R_new; //epistrefei th nea anadiorganwmenh sxesh

}

void print_R(relation* R){ //ektipwnei ta tuples ths relation pou dinetai
	int i, size=R->num_tuples;
	printf("Relation num_tuples: %d\n",size );
	for(i=0; i<size; i++){
		printf(" [%d]-> %d\n",i, R->tuples[i].key );
	}
}




