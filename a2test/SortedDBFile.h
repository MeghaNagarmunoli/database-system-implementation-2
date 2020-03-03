#ifndef SORTEDDBFILE_H
#define SORTEDDBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "GenericDBFile.h"
#include "BigQ.h"

// stub DBFile header..replace it with your own DBFile.h 
struct SortInfo { OrderMaker *myOrder; int runLength;
};

typedef enum {read, write} Mode; 

class SortedDBFile : public GenericDBFile {
public:
	char *filepath; 
	File file;
    off_t totalPageCount;
    off_t currentPage;
	Page page;
	ComparisonEngine comparator;
	SortInfo *sortInfo;
	Mode dbFileMode;
	int buffsz; // pipe cache size
	Pipe *input; 
	Pipe *output; 
	BigQ *bigQ;
	pthread_t myWorkerThread;

	bool bigQCreated;
	bool getNextCalledBefore;
	OrderMaker* queryOrder; 
	// OrderMaker myQueryOrder; 
	// OrderMaker mySortOrder;  
	// Record* pointerToLiteral;

	SortedDBFile (); 
	~SortedDBFile (); 
	//void *consumer (void *arg);
	int Create (const char *fpath, fType file_type, void *startup);
	int Open (const char *fpath);
	int Close ();

	void Load (Schema &myschema, const char *loadpath);
	int readOnePage(Record &fetchme);


	void MoveFirst ();
	void Add (Record &addme);
	//void HopefullyHarmless();
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);
	//void AddRecordToDiskFile(Record &addme);
	void MergeData();
	void AddRecordToDiskFile(File &tempFile, Page &tempPage, Record &rec, int &tempPageCount);
	bool BinarySearch(Record& fetchme,OrderMaker& leftOrder,Record& literal,OrderMaker& rightOrder);
	int GetNumofRecordPages();
	//bool binarySearchCompare (Record* left,Record* right);


};
#endif
