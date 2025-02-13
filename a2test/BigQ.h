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
#include <algorithm>

using namespace std;

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
	static void createFileWithRuns(Pipe*, vector<pair <int,int>>&, int&, int*, OrderMaker*, File&);
	
	

public:
	static void sortRun(vector<Record*> &records,File& new_file,int& gp_index,OrderMaker *sort_order){
	
		sort(records.begin(),records.end(),sort_func(sort_order));
		int c=0;
		Page page;// = new Page();
		for(Record *record : records) {
			if(page.Append(record)==0){
				new_file.AddPage(&page,(off_t)(gp_index++));
				page.EmptyItOut();
				page.Append(record);
				c++;	
			}
			else{
				c++;
			}						
		}
		//cout<<"G index end "<<gp_index<<"\n";
		new_file.AddPage(&page,(off_t)(gp_index++));	
		//cout<<"G index end "<<gp_index<<"\n";
		//delete tp;
	}

	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();
};



#endif