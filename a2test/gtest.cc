#include "gtest/gtest.h"
#include "DBFile.h"
#include <cstdio>
#include "test.h"
#include "BigQ.h"
#include <sys/types.h>
typedef struct yy_buffer_state *YY_BUFFER_STATE;

extern "C"
{
	int yyparse(void);							   // defined in y.tab.c
	YY_BUFFER_STATE yy_scan_string(char *str);	 // defined in lex.yy.c
	void yy_delete_buffer(YY_BUFFER_STATE buffer); // defined in lex.yy.c
}

TEST(test_open, testingopen){
    cout<<"Running Test"<<endl;
    Record *temp = new Record();
	int counter = 0;

	DBFile dbfile;
    relation *rel = new relation (part, new Schema ("catalog", "part"), "./testdata/");
	dbfile.Open (rel->path ());
	dbfile.MoveFirst ();
    vector<Record*> data;
	while (dbfile.GetNext (*temp) == 1) {
		counter += 1;

        Record *copyRecord = new Record;
        copyRecord->Copy(temp);
		data.push_back(copyRecord);
	}

    // for(Record *record : data) {
	// 		Schema ms("catalog", "part");
	// 		record->Print(&ms);
    // }

	dbfile.Close ();


    File new_file;

	char f_path[20] = "./testdata/test.bin";
	new_file.Open(0,f_path);
    new_file.Close();
    new_file.Open(1, f_path);
    int index = 0;
    OrderMaker sortorder;
    YY_BUFFER_STATE buffer = yy_scan_string("(p_partkey)");
	rel->get_sort_order (sortorder);
    sortorder.Print();
    BigQ::sortRun(data, new_file, index, &sortorder);
    cout<<"Running Test 3"<<endl;
    //new_file.GetPage()

    int err = 0;
	int i = 0;

	Record rec[2];
	Record *last = NULL, *prev = NULL;
    ComparisonEngine ceng;

    DBFile testDBFile;
    relation *testrel = new relation ("test", new Schema ("catalog", "part"), "./testdata/");
	testDBFile.Open (testrel->path ());
	//testDBFile.MoveFirst ();

    cout<<"Running Test 4"<<endl;


	while (testDBFile.GetNext(rec[i%2])==1) {
		prev = last;
		last = &rec[i%2];

		if (prev && last) {
			if (ceng.Compare (prev, last, &sortorder) == 1) {
				err++;
			}
		}
		i++;
	}
    EXPECT_EQ(0,err);
    cout<<"Number of errors : "<<err<<endl;
    remove("./testdata/test.bin");
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}