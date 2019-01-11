#include "jobScheduler.h"

using namespace std;

#define n 8

//arxikopoiei thn job queue kai to mutex kai to conditional variable pou xrisimopoiountai se authn
jqueue* init_queue(void){
	jqueue* queue=new jqueue;
	queue->start=NULL;
	queue->end=NULL;
	queue->numOfJobs=0;
	pthread_mutex_init (&(queue->job_mutex), NULL);
	pthread_cond_init (&(queue->job_cond), NULL);
	return queue;
}

//constauctor job
Job::Job(){};

//orizei ta arguments me ta opoia tha treksei to job tin function tou kai to id tou job
void Job::set_args(int jobId, arguments* args ){
	this->jobId=jobId;
	this->args=args;
}

//destructor job
Job::~Job(){};

//fuction tou job
int Job::function(void){
	cout<<"This is a job "<<this->jobId<<endl;
	return 0;
} 

//partitionJob

//constructor kai destructor tou partition job, derivative tou job
PartitionJob::PartitionJob():Job(){};
PartitionJob::~PartitionJob(){};

//function tou partition job , h reorder tou relation
int PartitionJob::function(void){
	if(this->args==NULL) {
		cout<<"Null arguments given"<<endl;
		return 1;
	}

	this->args->R_new=reorder_R(this->args->head, this->args->R, this->args->R_new, this->args->start, this->args->end);
	return 0;
} 

//JoinJob

//constructor kai destructor tou partition job, derivative tou job
JoinJob::JoinJob():Job(){};
JoinJob::~JoinJob(){};

//function tou join job, h join dio relations
int JoinJob::function(void){

	if(this->args==NULL) {
		cout<<"Null arguments given here"<<endl;
		return 1;
	}
	this->args->curr_res=join(this->args->list_head,this->args->curr_res,this->args->index,*(this->args->curr_R), *(this->args->curr_S),*(this->args->curr_Rp),*(this->args->curr_Sp),this->args->R_new,this->args->S_new);
	return 0;
} 

//constructor tou JobSch , pou arxikopoiei twn array apo threads , arxikopoiei thn oura, ta mutexes kai ta conditional variables kai dimiourgei ta threads
JobScheduler::JobScheduler(uint32_t numOfthreads){
	int rv, i;
	this->numOfthreads=numOfthreads;
	this->thr_arr=new pthread_t[this->numOfthreads];
	this->run=1;
	this->queue=init_queue();
	this->finished=0;

	pthread_mutex_init (&(this->jobs_mutex), NULL);
	pthread_cond_init (&(this->jobs_cond), NULL);


	for(i=0;i<(int)this->numOfthreads;i++){
		rv=pthread_create(&(this->thr_arr[i]), NULL, this->runThread,this);
		if(rv!=0) cout<<"kati paei lathos"<<endl;
	}

}

//prosthetei ena job sthn oura tou scheduler
void JobScheduler::pushJob(Job* j){

	pthread_mutex_lock (&(this->queue->job_mutex)); //kleidwnei thn oura apo opoiadipote alli drasthriothta kai prosthetei to job
	if(this->queue->start==NULL){
		this->queue->start=new jqueue_node;
		this->queue->start->job=j;
		this->queue->start->next=NULL;
		this->queue->end=this->queue->start;
		this->queue->numOfJobs+=1;
		
	}
	else{
		this->queue->end->next=new jqueue_node;
		this->queue->end->next->job=j;
		this->queue->end=this->queue->end->next;
		this->queue->end->next=NULL;
		this->queue->numOfJobs+=1;
	}
	                                   
    pthread_cond_signal (&(this->queue->job_cond));  //kanei signal me ton cond sta threads oti bike neo job                                  
    pthread_mutex_unlock (&(this->queue->job_mutex)); //kanei unlock to mutex ths ouras

}

//afairei ena job apo thn oura kai to epistrefei 
Job* JobScheduler::popJob(){
	Job* ret;
	jqueue_node* temp;
	if(this->queue->numOfJobs==0) return NULL;
	else if(this->queue->numOfJobs==1){
		ret=this->queue->start->job;
		this->queue->numOfJobs=0;	
		delete this->queue->start;
		this->queue->start=NULL;
	
		return ret;
	}
	else{
		ret=this->queue->start->job;
		temp=this->queue->start;
		this->queue->start=this->queue->start->next;
		delete temp;
		temp=NULL;
		this->queue->numOfJobs-=1;
		return ret;
	}
}

//ektipwnei thn oura
void  JobScheduler::printQueue(){
	jqueue_node* curr;
	if(this->queue!=NULL){
		if(this->queue->start!=NULL){
			curr=this->queue->start;
			while(curr!=NULL){
				cout<<"~Job id: "<<curr->job->jobId<<endl ;
				curr=curr->next;
			}
		}
		else cout<<"The queue is empty!"<<endl;
	}
		
}

//katadtrefei thn oura tou scheduler
void JobScheduler::destroyQueue(){
	jqueue_node* curr=this->queue->start;
	jqueue_node* temp;
	while(curr!=NULL){
		temp=curr;
		curr=curr->next;
		delete temp;
		temp=NULL;
	}
	delete queue;
	queue=NULL;
}

//orizei meta apo to telos kathe omadas diaforetikou job to finished 0, diladi oti i einai kai pali etoimo na dextei nea jobs
void JobScheduler::set_ready(){

	this->finished=0;
	
}


void* JobScheduler::runThread(void* sch){
	Job* j;
	//void* ret=NULL;
	
	JobScheduler* js=reinterpret_cast<JobScheduler*>(sch);
		
	while(js->run==1){ //perimenei mexri na parei kapoia douleia h na prepei na kanei exit
		pthread_mutex_lock(&(js->queue->job_mutex));

		while(js->queue->numOfJobs==0 ){  //perimenei mexri na parei kapoia douleia
			if(js->run==0)break; //prepei na kanei exit
			pthread_cond_wait(&(js->queue->job_cond), &(js->queue->job_mutex));
		}

		pthread_mutex_unlock(&(js->queue->job_mutex));
		if(js->run==0) return NULL; //epistrofh kai exit
			
		pthread_mutex_lock(&(js->queue->job_mutex));	

		j=js->popJob(); //afairw tin prwth douleia pou exw sthn oura
		pthread_mutex_unlock(&(js->queue->job_mutex));
		if(j!=NULL){ //an iparxei ontws douleia
			j->function();	//thn ektelei to thread		
		}

		pthread_mutex_lock(&(js->jobs_mutex));
			if(j!=NULL)js->finished+=1;  //auksanw ton arithmo apo jobs pou exoun idi ektelestei
		pthread_mutex_unlock(&(js->jobs_mutex));

		pthread_mutex_lock(&(js->jobs_mutex));		
		if(js->queue->numOfJobs==0){ //an den exw alla jobs pros to paron sthn oura
			                      
	        pthread_cond_signal(&js->jobs_cond); 
		}
		pthread_mutex_unlock(&(js->jobs_mutex));
			
		
	}
	return NULL;

}

//o barrier vazei ta threads na perimenoun oso iparxoun akoma jobs na ginoun
void JobScheduler::barrier(void* sch, int numOfJobs){
	JobScheduler* jsch=reinterpret_cast<JobScheduler*>(sch);
	
		pthread_mutex_lock (&(jsch->jobs_mutex));
		    while (jsch->finished < numOfJobs) {
		       pthread_cond_wait (&(jsch->jobs_cond), &(jsch->jobs_mutex));

		    }
	    	
		pthread_mutex_unlock (&(jsch->jobs_mutex));

	
}

//ean den iparxoun alla jobs na parei i queue kaleitai, kai i run flag ginetai 0 , eidopoiei ta threads oti teleiwsan ta jobs, kai kanei join ta threads
void JobScheduler::finishJobs(void* sch){
	int i;
	JobScheduler* jsch=reinterpret_cast<JobScheduler*>(sch);

	jsch->run = 0; //den uparxoun alla jobs na parei h queue opote 
    pthread_mutex_lock (&(jsch->queue->job_mutex));         
    pthread_cond_broadcast (&(jsch->queue->job_cond));                                    
    pthread_mutex_unlock (&(jsch->queue->job_mutex));

    for (i = 0; i < (int)jsch->numOfthreads; i++) {
    	pthread_join (jsch->thr_arr[i], NULL);  

	}

}

//destructor tou scheduler
JobScheduler::~JobScheduler(){

    delete[] thr_arr;
    this->destroyQueue();
}

//constructor histogram job
HistJob::HistJob():Job(){};

//destructor histogram job
HistJob::~HistJob(){};

//fuction tou histograp job, diladi to update_hist
int HistJob::function(void){
	args->hist_list[this->jobId]= update_hist(this->args->start, this->args->end, this->args->rel);
	return 1;	
}
