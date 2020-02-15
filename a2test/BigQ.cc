#include "BigQ.h"
#include <pthread.h>
#include "Comparison.h"
#include <queue>
#include <fstream>
#include <string.h>


typedef struct {
	Pipe *inPipe;
	Pipe *outPipe;
	OrderMaker *order;
	int runLen;
}inpututil;

OrderMaker* om;
ComparisonEngine ce;

class Comp{
	public:
		bool operator()(Record  right, Record left){
			return ce.Compare(&right, &left, om) == 1;

		}
};

void *worker(void *attr){
	inpututil *t = (inpututil *) attr;

	Record temp;
	Page sortPage;
	char **numLenPages = new char*[t->runLen];
	int pageCount = 0;

	while(pageCount<t->runLen) {
	
		t->inPipe->Remove(&temp);
		if (!sortPage.Append(&temp)){
			sortPage.ToBinary(numLenPages[pageCount++]);
			sortPage.EmptyItOut();
		}
		sortPage.EmptyItOut();
	}
	
	//TODO: remove char ** and just use priorty queue

	priority_queue<Record, vector<Record>, Comp> queue;

	for(int i = 0; i < t->runLen; i++){
		sortPage.FromBinary(numLenPages[i]);
		while (sortPage.GetFirst(&temp)) 
		{
			queue.push(temp);
		}	
	}

	sortPage.EmptyItOut();
	pageCount = 0;

	File tempFile;
	const char* tempFileName = 	"tempFile.txt";
	char *path = const_cast<char *>(tempFileName);
	tempFile.Open(0, path);
	//tempFile.MoveFirst();

	while(!queue.empty()){
		temp = queue.top();
		temp.Print();
		if (!sortPage.Append(&temp)){
			tempFile.AddPage(&sortPage, tempFile.GetLength());
			sortPage.EmptyItOut();
		}
		sortPage.EmptyItOut();
		queue.pop();

	}
	tempFile.Close();

	t->outPipe->ShutDown ();
}

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
	// read data from in pipe sort them into runlen pages
	pthread_t thread3;
	
	om = &sortorder;

	inpututil input = {&in, &out, &sortorder, runlen};

	pthread_create (&thread3, NULL, worker, (void *)&input);

    // construct priority queue over sorted runs and dump sorted data 
 	// into the out pipe

    // finally shut down the out pipe
	
}


BigQ::~BigQ () {
}

 