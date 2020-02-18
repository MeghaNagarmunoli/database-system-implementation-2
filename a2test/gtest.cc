#include "gtest/gtest.h"
#include "DBFile.h"
#include <cstdio>
#include "test.h"
#include "BigQ.h"
#include <sys/types.h>
#include "Pipe.h"
#include <pthread.h>
#include <unistd.h>
typedef struct yy_buffer_state *YY_BUFFER_STATE;

extern "C"
{
	int yyparse(void);							   // defined in y.tab.c
	YY_BUFFER_STATE yy_scan_string(char *str);	 // defined in lex.yy.c
	void yy_delete_buffer(YY_BUFFER_STATE buffer); // defined in lex.yy.c
}

int line = 0;

TEST(test_sorting_for_a_run, testsorting){
    cout<<"Running Test"<<endl;
    Record *temp = new Record();
	int counter = 0;

	DBFile dbfile;
    relation *rel1 = new relation (part, new Schema ("catalog", "part"), "./testdata/");
	dbfile.Open (rel1->path ());
	dbfile.MoveFirst ();
    vector<Record*> data;
	while (dbfile.GetNext (*temp) == 1) {
		counter += 1;

        Record *copyRecord = new Record;
        copyRecord->Copy(temp);
		data.push_back(copyRecord);
	}

	dbfile.Close ();


    File new_file;

	char f_path[20] = "./testdata/test.bin";
	new_file.Open(0,f_path);
    new_file.Close();
    new_file.Open(1, f_path);
    int index = 0;
    OrderMaker sortorder;
    YY_BUFFER_STATE buffer = yy_scan_string("(p_partkey)");
	rel1->get_sort_order (sortorder);
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




TEST(test_sorting_for_a_run_1, testingsort){
    cout<<"Running Test 1"<<endl;
    Record *temp = new Record();
	int counter = 0;

	DBFile dbfile;
    relation *rel1 = new relation (nation, new Schema ("catalog", "nation"), "./testdata/");
	dbfile.Open (rel1->path ());
	dbfile.MoveFirst ();
    vector<Record*> data;
	while (dbfile.GetNext (*temp) == 1) {
		counter += 1;

        Record *copyRecord = new Record;
        copyRecord->Copy(temp);
		data.push_back(copyRecord);
	}

	dbfile.Close ();


    File new_file;

	char f_path[20] = "./testdata/test.bin";
	new_file.Open(0,f_path);
    new_file.Close();
    new_file.Open(1, f_path);
    int index = 0;
    OrderMaker sortorder;
    YY_BUFFER_STATE buffer = yy_scan_string("(n_regionkey)");
	rel1->get_sort_order (sortorder);
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
    relation *testrel = new relation ("test", new Schema ("catalog", "nation"), "./testdata/");
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


void *producer (void *arg) {

	Pipe *myPipe = (Pipe *) arg;
    relation *rel1;
	//cout << " producer: opened DBFile ******" << rel->path () << endl;
    if(line == 0){
        rel1 = new relation (nation, new Schema ("catalog", "nation"), "./testdata/");
    }
    else{
        rel1 = new relation (lineitem, new Schema ("catalog", "lineitem"), "./testdata/");
    }

	Record temp;
	int counter = 0;

	DBFile dbfile;
	dbfile.Open (rel1->path ());
	cout << " producer: opened DBFile " << rel1->path () << endl;
	dbfile.MoveFirst ();

	while (dbfile.GetNext (temp) == 1) {
		counter += 1;
		if (counter%100000 == 0) {
			 cerr << " producer: " << counter << endl;	
		}
		myPipe->Insert (&temp);
	}

	dbfile.Close ();
	myPipe->ShutDown ();

	cout << " producer: inserted " << counter << " recs into the pipe\n";
}

void *consumer (void *arg) {
	
	testutil *t = (testutil *) arg;

	ComparisonEngine ceng;

	DBFile dbfile;
	char outfile[100];

	int err = 0;
	int i = 0;

	Record rec[2];
	Record *last = NULL, *prev = NULL;

	while (t->pipe->Remove (&rec[i%2])) {
		prev = last;
		last = &rec[i%2];

		if (prev && last) {
			if (ceng.Compare (prev, last, t->order) == 1) {
				err++;
			}

		}

		i++;
	}
    EXPECT_EQ(err,0);

}


TEST(test_integration1, intergration_test){
    int runlen = 8;

    relation *rel1 = new relation (nation, new Schema ("catalog", "nation"), "./testdata/");
	// sort order for records
	OrderMaker sortorder;
    YY_BUFFER_STATE buffer = yy_scan_string("(n_regionkey)");
	rel1->get_sort_order (sortorder);

	int buffsz = 100; // pipe cache size
	Pipe input (buffsz);
	Pipe output (buffsz);

	// thread to dump data into the input pipe (for BigQ's consumption)
	pthread_t thread1;
	pthread_create (&thread1, NULL, producer, (void *)&input);

	// thread to read sorted data from output pipe (dumped by BigQ)

	pthread_t thread2;
	testutil tutil = {&output, &sortorder, false, false};

	pthread_create (&thread2, NULL, consumer, (void *)&tutil);

	BigQ bq (input, output, sortorder, runlen);

	pthread_join (thread1, NULL);
	pthread_join (thread2, NULL);

}

TEST(test_integration2, intergration_test){
    int runlen = 8;

    line = 1;
    relation *rel1 = new relation (lineitem, new Schema ("catalog", "lineitem"), "./testdata/");
	// sort order for records
	OrderMaker sortorder;
    YY_BUFFER_STATE buffer = yy_scan_string("(l_partkey)");
	rel1->get_sort_order (sortorder);

	int buffsz = 100; // pipe cache size
	Pipe input (buffsz);
	Pipe output (buffsz);

	// thread to dump data into the input pipe (for BigQ's consumption)
	pthread_t thread1;
	pthread_create (&thread1, NULL, producer, (void *)&input);

	// thread to read sorted data from output pipe (dumped by BigQ)

	pthread_t thread2;
	testutil tutil = {&output, &sortorder, false, false};

	pthread_create (&thread2, NULL, consumer, (void *)&tutil);

	BigQ bq (input, output, sortorder, runlen);

	pthread_join (thread1, NULL);
	pthread_join (thread2, NULL);

}


int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}