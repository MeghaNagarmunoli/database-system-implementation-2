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

public:
	SortedDBFile (); 
	~SortedDBFile (); 
	//void *consumer (void *arg);
	int Create (const char *fpath, fType file_type, void *startup);
	int Open (const char *fpath);
	int Close ();

	void Load (Schema &myschema, const char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	//void HopefullyHarmless();
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);
	//void AddRecordToDiskFile(Record &addme);

};
#endif
