#include "gtest/gtest.h"
#include "DBFile.h"
#include <cstdio>
#include "test.h"
#include "BigQ.h"
#include <sys/types.h>
#include "Pipe.h"
#include <pthread.h>
#include <unistd.h>
#include "DBFile.h"
#include "HeapDBFile.h"
#include "SortedDBFile.h"
#include "GenericDBFile.h"
#include <fstream>
typedef struct yy_buffer_state *YY_BUFFER_STATE;

extern "C"
{
	int yyparse(void);							   // defined in y.tab.c
	YY_BUFFER_STATE yy_scan_string(char *str);	 // defined in lex.yy.c
	void yy_delete_buffer(YY_BUFFER_STATE buffer); // defined in lex.yy.c
}

int add_data (DBFile &dbfile, FILE *src, int numrecs, relation *rel) {
	
	Record temp;
	int proc = 0;
	int xx = 20000;
	int res = 0;
	while ((res = temp.SuckNextRecord (rel->schema (), src)) && ++proc < numrecs) {
		dbfile.Add (temp);
		if (proc == xx) cerr << "\t ";
		if (proc % xx == 0) cerr << ".";
	}
	if(res)
		dbfile.Add (temp);
	dbfile.Close();
	return proc;
}

TEST(testing_merge_code, testingMerge){
    
    Record *temp = new Record();
	int counter = 0;
	relation *rel1 = new relation (nation, new Schema ("catalog", "nation"), "./testdata/");
	ifstream  src("./testdata/nationOriginal.bin", std::ios::binary);
    ofstream  dst("./testdata/nation.bin",   std::ios::binary);
    dst << src.rdbuf();
	src.close();
	dst.close();

	char tbl_path[100];
	cout<<tpch_dir<<endl;
	sprintf (tbl_path, "%s%s.tbl", "./testdata/", rel1->name()); 
	cout << " input from file : " << tbl_path << endl;
    FILE *tblfile = fopen (tbl_path, "r");

	DBFile dbfile;
    
	cout<<rel1->path()<<endl;
	dbfile.Open (rel1->path ());
	// Data in the file and the data from the pipe with get merged after it.
	add_data(dbfile,tblfile,10, rel1);

	// Now we will read the output file and see if its in sorted order or not.
	OrderMaker sortorder;
    YY_BUFFER_STATE buffer = yy_scan_string("(n_regionkey)");
	rel1->get_sort_order (sortorder);
	ComparisonEngine ceng;
	Record rec[2];
	Record *last = NULL, *prev = NULL;
	int i=0;
	DBFile dbMerged;
	dbMerged.Open (rel1->path ());
	dbMerged.MoveFirst();
	int err = 0;
	while (dbMerged.GetNext(rec[i%2]) == 1) {
		prev = last;
		last = &rec[i%2];

		if (prev && last) {
			if (ceng.Compare (prev, last, &sortorder) == 1) {
				err++;
			}
		}
		i++;
	}

	cout << "Read " << i << " recs from the newly merged file\n";

	dbfile.Close ();
	remove("./testdata/nation.bin");
	EXPECT_EQ(0,err);
	EXPECT_EQ(20,i);
}






int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}