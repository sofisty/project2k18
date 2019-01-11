#ifndef JOBSCHEDULER_H
#define JOBSCHEDULER_H

#include <iostream>
#include <cstdlib>
#include <pthread.h>
#include <semaphore.h> 
#include <unistd.h> 

#include <stdio.h>
#include "results.h"
#include "hash1.h"

typedef struct arguments{
	int n;
	relation* rel;
	int start;
	int end;
	hist_node** hist_list;
	psum_node* head;
	relation* R;
	relation* R_new;
	relation* S_new;
	int hash_n;
	int index;
	result** list_head;
	result* curr_res;
	hist_node* curr_R;
	hist_node* curr_S;
	psum_node* curr_Rp;
	psum_node* curr_Sp;
	class JobScheduler* sch;
}arguments;


typedef struct jqueue_node{
	class Job* job;
	struct jqueue_node* next;
}jqueue_node;

typedef struct jqueue{
	jqueue_node* start;
	jqueue_node* end;
	int numOfJobs;

	pthread_mutex_t    job_mutex; //wait
	pthread_mutex_t	   pop_mutex;
	pthread_cond_t 	   job_cond; 
}jqueue;

jqueue* init_queue(void);


class Job{
	public:
		int jobId;
		arguments* args;
		//synarthseis
		virtual int function(void);
		Job();
		~Job();
		void set_args(int jobId, arguments* args );
};

class HistJob: public Job{
	public:
		virtual int function(void) override;

		HistJob();
		~HistJob();

};

//partition Job
class PartitionJob: public Job{
	public:
		virtual int function(void) override;

		PartitionJob();
		~PartitionJob();

};

//join Job
class JoinJob: public Job{
	public:
		virtual int function(void) override;

		JoinJob();
		~JoinJob();

};


class JobScheduler{		
	public:
		int run;
		int finished;
		uint32_t numOfthreads;
		pthread_t* thr_arr;
		pthread_mutex_t jobs_mutex;
		pthread_cond_t jobs_cond;		
		jqueue* queue;
		

		JobScheduler(uint32_t numOfthreads);
		~JobScheduler();

		void pushJob(Job* j);
		Job* popJob();
		void printQueue();
		void destroyQueue();

		static void* runThread(void*);
		void barrier(void* sch, int numOfJobs);
		void set_ready();
		void finishJobs(void* sch);
};


#endif