#include "jobScheduler.h"

using namespace std;

#define n 8
int test(int i){
	printf("i: %d\n", i);
	return i;
}
jqueue* init_queue(void){
	jqueue* queue=new jqueue;
	queue->start=NULL;
	queue->end=NULL;
	queue->numOfJobs=0;
	pthread_mutex_init (&(queue->job_mutex), NULL);
	pthread_cond_init (&(queue->job_cond), NULL);
	return queue;
}

Job::Job(){
	
}

void Job::set_args(int jobId, arguments* args ){
	this->jobId=jobId;
	this->args=args;
}

Job::~Job(){
	/*if(args!=NULL){
		delete args;
	}*/
};

int Job::function(void){
	cout<<"This is a job "<<this->jobId<<endl;
	return 0;
} 

//partitionJob
PartitionJob::PartitionJob():Job(){};
PartitionJob::~PartitionJob(){};

int PartitionJob::function(void){
	//cout<<"This is a partitionJob"<<this->jobId<<endl;
	if(this->args==NULL) {
		cout<<"Null arguments given"<<endl;
		return 1;
	}

	this->args->R_new=reorder_R(this->args->head, this->args->R, this->args->R_new, this->args->start, this->args->end);
	return 0;
} 

//JoinJob
JoinJob::JoinJob():Job(){};
JoinJob::~JoinJob(){};

int JoinJob::function(void){
	//cout<<"This is a partitionJob"<<this->jobId<<endl;
	if(this->args==NULL) {
		cout<<"Null arguments given here"<<endl;
		//exit(1);
		return 1;
	}
	//join(result** head,result* curr_res, int index,hist_node curr_R, hist_node curr_S, psum_node curr_Rp, psum_node curr_Sp, relation* R_new, relation* S_new);
	this->args->curr_res=join(this->args->list_head,this->args->curr_res,this->args->index,*(this->args->curr_R), *(this->args->curr_S),*(this->args->curr_Rp),*(this->args->curr_Sp),this->args->R_new,this->args->S_new);
	return 0;
} 

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
	//cout<<"Now threads are created"<<endl;

	
}


void JobScheduler::pushJob(Job* j){
	pthread_mutex_lock (&(this->queue->job_mutex));  
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
	                                   
    pthread_cond_signal (&(this->queue->job_cond));                                    
    pthread_mutex_unlock (&(this->queue->job_mutex));

}

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
	//pthread_cond_destroy (&(this->queue->job_cond));
}

void JobScheduler::set_ready(){

	this->finished=0;
	
}

void* JobScheduler::runThread(void* sch){
	Job* j;
	//void* ret=NULL;
	
	JobScheduler* js=reinterpret_cast<JobScheduler*>(sch);
		
	while(js->run==1){ //perimenei mexri na parei kapoia douleia h na prepei na kanei exit
		pthread_mutex_lock(&(js->queue->job_mutex));
		//cout<<"This is thread with id: "<<pthread_self()<<endl;
		while(js->queue->numOfJobs==0 ){ 
			if(js->run==0)break; //prepei na kanei exit
			pthread_cond_wait(&(js->queue->job_cond), &(js->queue->job_mutex));
		}
		pthread_mutex_unlock(&(js->queue->job_mutex));
		if(js->run==0) return NULL; //epistrofh kai exit
			
		pthread_mutex_lock(&(js->queue->job_mutex));	 
		//cout<<"Kanw pop thread: "<<pthread_self()<<endl;
		j=js->popJob();
		//cout<<"POP JOB ID : "<<j->jobId<<endl; 
		pthread_mutex_unlock(&(js->queue->job_mutex));
		//if(j==NULL){cout<<"NOP"<<endl;}
		if(j!=NULL){
			
			j->function();
			//cout<<"HIST JOB ID : "<<j->jobId<<endl; 
			
		}
		//cout<<"---------------------------"<<js->queue->numOfJobs<<endl;
		pthread_mutex_lock(&(js->jobs_mutex));
			if(j!=NULL)js->finished+=1;  
		pthread_mutex_unlock(&(js->jobs_mutex));

		pthread_mutex_lock(&(js->jobs_mutex));
		//if (js->finished==3) { 		
		if(js->queue->numOfJobs==0){	
			//cout<<"FINISHED "<<js->finished<<endl;
			//cout<<"ekana signal: "<<pthread_self()<<endl;
			                      
	        pthread_cond_signal(&js->jobs_cond);
		}
		pthread_mutex_unlock(&(js->jobs_mutex));
			
		
	}
	return NULL;

}

void JobScheduler::barrier(void* sch, int numOfJobs){
	JobScheduler* jsch=reinterpret_cast<JobScheduler*>(sch);

	/*pthread_mutex_lock (&(jsch->queue->job_mutex));
	pthread_cond_broadcast (&(jsch->queue->job_cond));
	pthread_mutex_unlock (&(jsch->queue->job_mutex)); */
	
	
		pthread_mutex_lock (&(jsch->jobs_mutex));
	  		//cout<<"NUMOFJOBS "<<jsch->queue->numOfJobs<<endl;
		    while (jsch->finished < numOfJobs) {
		       pthread_cond_wait (&(jsch->jobs_cond), &(jsch->jobs_mutex));

		    }
	    	
		pthread_mutex_unlock (&(jsch->jobs_mutex));

	
}

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


JobScheduler::~JobScheduler(){
	int i;
    delete[] thr_arr;

    this->destroyQueue();
    /*pthread_cond_destroy (&(this->jobs_cond));
    */
}

HistJob::HistJob():Job(){

	//if(args!=NULL)cout<<"~CONSTRUCTOR~ start: "<<args->start<<" and end: "<<args->end<<endl; 
};

HistJob::~HistJob(){};

int HistJob::function(void){
	//cout<<"HistJob "<<this->jobId<<" : "<<this->args->start<<" + "<<this->args->end <<endl;
	args->hist_list[this->jobId]= update_hist(this->args->start, this->args->end, this->args->rel);
	return 1;
	//print_hist( hist);
	//return (void*) hist;
	
}



/*
int main (void){
	int i, rv;
	arguments* args=NULL;
	JobScheduler sch= JobScheduler(3);
	JobScheduler *jsch=&sch;
	Job* job1;
	//Work work=Work(1,&args);
	Work work1=Work(1,args);
	Work work2=Work(2,args);
	Work work3=Work(3,args);
	Work work4=Work(4,args);

	Work1 work11=Work1(1,args);
	Work1 work12=Work1(2,args);
	Work1 work13=Work1(3,args);
	Work1 work14=Work1(4,args);

	job1=&work1;
	jsch->pushJob(job1);
	job1=&work2;
	jsch->pushJob(job1);
	job1=&work3;
	jsch->pushJob(job1);
	
	job1=&work4;
	jsch->pushJob(job1);
	//jsch->printQueue();


	jsch->barrier(jsch);


	//load work

	job1=&work11;
	jsch->pushJob(job1);
	job1=&work12;
	jsch->pushJob(job1);

	
	job1=&work13;
	jsch->pushJob(job1);
	job1=&work14;
	jsch->pushJob(job1);

	//jsch->printQueue();

	jsch->barrier(jsch);

	jsch->finishJobs(jsch);


}
*/
