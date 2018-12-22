#ifndef JOBSCHEDULER_H
#define JOBSCHEDULER_H

#include <iostream>
#include <cstdlib>
#include <pthread.h>

#include "results.h"
#include "hash1.h"


//hist_node* update_hist(hist_node* hist, relation* R, int n)


typedef struct arguments{
	int i;
	double d;
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

class Job{
	public:
		int jobId;
		arguments* args;

		//synarthseis
		virtual int function(void);
		Job(int jobId, arguments* args);
		~Job();
};

class Work: public Job{
	public:
		virtual int function(void) override;

		Work(int jobId, arguments* args);
		~Work();

};

class JobScheduler{		
	public:
		int run;
		uint32_t numOfthreads;
		pthread_t* thr_arr;
		pthread_mutex_t jobs_mutex;
		pthread_cond_t jobs_cond;		
		jqueue* queue;
		void* result_list;

		JobScheduler(uint32_t numOfthreads);
		~JobScheduler();

		void pushJob(Job* j);
		Job* popJob();
		void printQueue();
		void destroyQueue();
		static void* runThread(void*);
};


#endif