#include "jobScheduler.h"

using namespace std;

int test(int i){
	printf("i: %d\n", i);
	return i;
}

Job::Job(int jobId, arguments* args){
	this->jobId=jobId;
	this->args=args;
}

Job::~Job(){};

int Job::function(void){
	cout<<"This is a job "<<this->jobId<<endl;
	return 0;
} 

JobScheduler::JobScheduler(uint32_t numOfthreads){
	int rv, i;
	this->numOfthreads=numOfthreads;
	this->thr_arr=new pthread_t[this->numOfthreads];
	this->result_list=NULL;
	this->queue=new jqueue;

	//------initialliaze queue-------
	this->queue->start=NULL;
	this->queue->end=NULL;
	this->queue->numOfJobs=0;
	pthread_mutex_init (&(this->queue->job_mutex), NULL);
	pthread_mutex_init (&(this->queue->pop_mutex), NULL);
	pthread_cond_init (&(this->queue->job_cond), NULL);
	//-------------------------------

	pthread_mutex_init (&(this->jobs_mutex), NULL);
	pthread_cond_init (&(this->jobs_cond), NULL);
	
}

void JobScheduler::pushJob(Job* j){
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
		this->queue->numOfJobs+=1;
		this->queue->end->next=NULL;

	}

}

Job* JobScheduler::popJob(){
	Job* ret;
	jqueue_node* temp;
	if(this->queue->numOfJobs==0) return NULL;
	else if(this->queue->numOfJobs==1){
		ret=this->queue->start->job;
		this->queue->numOfJobs-=1;
		temp=this->queue->start;
		this->queue->start=this->queue->start->next;
		delete temp;
		temp=NULL;
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
	if(this->queue->start!=NULL){
		curr=this->queue->start;
		while(curr!=NULL){
			curr->job->function();;
			curr=curr->next;
		}
	}
	else cout<<"The queue is empty!"<<endl;
	
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

	pthread_cond_destroy (&(this->queue->job_cond));
}

void* JobScheduler::runThread(void* sch){
	Job* j;
	
	JobScheduler* js=reinterpret_cast<JobScheduler*>(sch);
	
	pthread_mutex_lock(&(js->queue->job_mutex));
	while(js->queue->numOfJobs==0){
		pthread_cond_wait(&(js->queue->job_cond), &(js->queue->job_mutex));
	}
	pthread_mutex_unlock(&(js->queue->job_mutex));

	pthread_mutex_lock(&(js->queue->pop_mutex));
	j=js->popJob();
	pthread_mutex_unlock(&(js->queue->pop_mutex));

	if(j!=NULL){
		//cout<<"This is thread with id: "<<pthread_self()<<endl;
		j->function();
	}
	//cout<<"---------------------------"<<js->queue->numOfJobs<<endl;
	pthread_mutex_lock(&(js->jobs_mutex));
	if (js->queue->numOfJobs==0) {     
		cout<<"Eimai edw"<<endl;                      
        pthread_cond_signal (&js->jobs_cond);
	}
	pthread_mutex_unlock(&(js->jobs_mutex));

	return NULL;
}

JobScheduler::~JobScheduler(){
	int i;

    pthread_mutex_lock (&(this->queue->job_mutex));                                     
    pthread_cond_broadcast (&(this->queue->job_cond));                                    
    pthread_mutex_unlock (&(this->queue->job_mutex));

    for (i = 0; i<this->numOfthreads; i++) {
        pthread_join (this->thr_arr[i], NULL);                              
    }
    delete[] thr_arr;

    this->destroyQueue();
    pthread_cond_destroy (&(this->jobs_cond));
}


Work::Work(int jobId, arguments* args):Job(jobId, args){};
Work::~Work(){};

int Work::function(void){
	cout<<"This is work"<<this->jobId<<endl;
	return test(5);
}

int main (void){
	int i, rv;
	arguments args;
	JobScheduler* jsch= new JobScheduler(3);
	Job* job1=new Job(0, &args);
	//Work work=Work(1,&args);
	Work work1=Work(1,&args);
	Work work2=Work(2,&args);
	Work work3=Work(3,&args);
	Work work4=Work(4,&args);

	job1=&work1;
	jsch->pushJob(job1);
	job1=&work2;
	jsch->pushJob(job1);
	job1=&work3;
	jsch->pushJob(job1);
	job1=&work4;
	jsch->pushJob(job1);
	jsch->printQueue();

	cout<<"Now threads are created"<<endl;
	for(i=0;i<jsch->numOfthreads;i++){

		rv=pthread_create(&(jsch->thr_arr[i]), NULL, jsch->runThread,jsch);
		if(rv!=0) cout<<"kati paei lathos"<<endl;
	}

	//jsch->printQueue();	

	/*cout<<"-----Arxizoun ta pop"<<endl;

	job1=jsch->popJob();
	job1->function();
	//jsch->printQueue();
	cout<<"---------------"<<endl;
	job1=jsch->popJob();
	job1->function();
	//jsch->printQueue();
	cout<<"---------------"<<endl;
	job1=jsch->popJob();
	job1->function();
	//jsch->printQueue();
	cout<<"---------------"<<endl;
	job1=jsch->popJob();
	job1->function();
	//jsch->printQueue();*/


	/*
	for(i=0;i<jsch->numOfthreads;i++){
		rv=pthread_create(&(jsch->thr_arr[i]), NULL, PrintHello, (void *)i);
	}*/
}

