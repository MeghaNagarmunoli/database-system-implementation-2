#ifndef HEAPDBFILE_H
#define HEAPDBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "GenericDBFile.h"


// stub DBFile header..replace it with your own DBFile.h 

class HeapDBFile : public GenericDBFile {
	char *filepath; 
	File file;
    off_t totalPageCount;
    off_t currentPage;
	Page page;
	ComparisonEngine comparator;

public:
	HeapDBFile (); 
	~HeapDBFile (); 
	int Create (const char *fpath, fType file_type, void *startup);
	int Open (const char *fpath);
	int Close ();

	void Load (Schema &myschema, const char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	//void HopefullyHarmless();
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);

};
#endif
