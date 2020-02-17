#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include "DBFile.h"
#include "ComparisonEngine.h"
#include <vector>
#include <queue>

using namespace std;

class BigQ {

	//DataStructures 
	//Record temp to read in records from pipes
	
	//Record temp;
	vector<DBFile*> *runs;
	int no_runs;
	pthread_t worker;
	int page_Index;
	char *f_name;
	
	struct args_phase1_struct {                                                   
		
		Pipe *input;
		Pipe *output;
		OrderMaker *sort_order;
		int *run_length;
	
	}args_phase1;

	typedef struct args_phase1_struct args_phase1_struct;

	static void* SortAndMerge(void* arg);
	static void quicksort(vector<Record> &rb, int left, int right,OrderMaker &sortorder);
	static void sort_run(Page*,int,File&,int&,OrderMaker *);
	static void sortRun(vector<Record*>&, File& ,int& gpindex,OrderMaker*);
//	static bool sort_func(Record &,Record &,OrderMaker &sortorder);	
	/*Deprecated: Replaced by DBFile , no need for indexing
	//Record *recordBuff;
	//int pageLength = 0;//no of recrods per page
	//Page *buffer;*/
	

public:

	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();
};

typedef struct rwrap{
	
	Record rec;
	int run;

}rwrap;


class sort_func{

private:

	OrderMaker *sort_order;

public:

	sort_func(OrderMaker *order){
		this->sort_order = order;
	}

	sort_func(){};

	bool operator()(Record *one,Record *two) const{


		ComparisonEngine *compare;

		if(compare->Compare(one,two,this->sort_order)<0){
			return true;
		}

		else{
			return false;
		}
	}


	bool operator()(rwrap *one,rwrap *two) const{

		ComparisonEngine *compare;

		if(compare->Compare(&(one->rec),&(two->rec),this->sort_order)<0){
			//return true;
			return false;
		}

		else{
			//return false;
			return true;
		}
	}

	

};

#endif