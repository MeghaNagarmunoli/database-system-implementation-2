#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Defs.h"
#include <iostream>
#include <stdio.h>
#include <string.h>
#include "SortedDBFile.h"
#include "GenericDBFile.h"
#include <fstream>
#include <cstdio>
#include <sstream>
#include <cstring>
using namespace std;


bool binarySearchCompare (Record* left,Record* right);
OrderMaker myQueryOrder; 
	OrderMaker mySortOrder;  
	Record* pointerToLiteral;
struct threadArgs {Pipe &input; const char *filepath;Schema *mySchema;};

typedef struct{
  Pipe* inputPipe;
  Pipe* outputPipe;
  OrderMaker* sortOrder;
  int runlen;
} myWorkerUtil;

void* myWorkerRoutine(void* ptr){
  myWorkerUtil* myT = (myWorkerUtil*) ptr;
  // cout << " sorted.myworkerroutine " << myT->runlen << endl; // debug
  new BigQ(*(myT->inputPipe),*(myT->outputPipe),*(myT->sortOrder),myT->runlen); // the BigQ constructor spawns a thread and waits on
                                                           //  1. The input pipe to shut down
                                                           //  2. The TPMMS to start and finish.
                                                           //  3. The output pipe to be emptied.
                                                           // 1 won't happen till switchToReading(). Hence, we need to put
                                                           // the constructor in its own queue right now so it doesn't
                                                           // block Sorted.cc
  return 0; // http:// stackoverflow.com/a/5761837: Pthreads departs from the standard unix return code of -1 on error convention. It returns 0 on success and a positive integer code on error.
}
// stub file .. replace it with your own DBFile.cc
void *producer (void *arg) {
    threadArgs* inputArgs = (threadArgs*) arg;

    FILE *tableFile = fopen(inputArgs->filepath,"r");
    if(tableFile == NULL) {
        cerr<<"Failed to open file at loadpath:"<<inputArgs->filepath<<" . Maybe the file doesn't exist at that location!!!"<<endl;
    }
    Record temp;
    off_t numberOfrecords = 0;
    while (temp.SuckNextRecord (inputArgs->mySchema, tableFile)) {
        numberOfrecords++;
        inputArgs->input.Insert(&temp);
    } 
	inputArgs->input.ShutDown ();
	cout << " producer: inserted " << numberOfrecords << " recs into the pipe\n";
}

void* consumer (void *arg) {
	
	threadArgs *t = (threadArgs *) arg;
	DBFile dbfile;
	char outfile[100];

    sprintf (outfile, "%s", t->filepath);
    cout <<  "This is the output file location " <<outfile << endl;
    dbfile.Create (outfile, sorted, NULL);
    dbfile.Open(outfile);

	int err = 0;
	int i = 0;

	Record rec;

	while (t->input.Remove (&rec)) {
        dbfile.Add (rec);
	}

	cout << " consumer: removed " << i << " recs from the pipe\n";

    dbfile.Close ();

	cout << " consumer: " << (i - err) << " recs out of " << i << " recs in sorted order \n";
	if (err) {
		cerr << " consumer: " <<  err << " recs failed sorted order test \n" << endl;
	}
}


SortedDBFile::SortedDBFile () {
    totalPageCount = 0;
    currentPage = 0;
    filepath = NULL;
    dbFileMode = read;
    bigQ = NULL;
    buffsz = 100;
    Pipe input(buffsz);
    Pipe output(buffsz);
    bigQCreated  =false;
    getNextCalledBefore = false;
}

SortedDBFile::~SortedDBFile () {
    delete(filepath);
}
int SortedDBFile::Create (const char *f_path, fType f_type, void *startup) {
    
    if(f_path == NULL || f_path[0] == '\0') {
        cerr<<"Path to create file is null!!!"<<endl;
        return 0;
    }
    cout<<"Meta Sorted file write"<<endl;
    // Create meta data for sorted file.
    char meta[] = ".metaSorted";
    char *c = new char[strlen(f_path) + 11]{};
    strcpy(c, f_path);
    strcat(c, meta);
    // Write the metadata to file


    ofstream metaSortedFile (c);
   // FILE *metaSortedFile;
   // metaSortedFile = fopen(c, "w");
    sortInfo = (SortInfo*)startup;

    char orderMakerMetaData[1000]; 
    sortInfo->myOrder->PrinttoString(orderMakerMetaData);
    cout<<"Order maker :"<<orderMakerMetaData<<endl;

    if(metaSortedFile.is_open()) {
        metaSortedFile << sortInfo->runLength <<endl;
        metaSortedFile << orderMakerMetaData <<endl;       
    }
    //fprintf (metaSortedFile, "%s\n",0,sortInfo);
    //fwrite(orderMakerMetaData, sizeof(struct SortInfo), 1, metaSortedFile);    
    metaFilefile.Close();
   
   
    // cout<<"Sort Order!!"<<endl;
    // sortInfo->myOrder->Print();
    // cout<<"Printed sort order!!!"<<endl;
    // //FILE *metaSortedFile;
   
   
    // FILE *testFile = fopen(c, "r");
    // cout<<"File path :"<<c<<endl;
    // SortInfo s;
    // fread(&s, sizeof(struct SortInfo), 1, testFile);
    // cout<<"printing sort order after reading from file"<<endl;
    // s.myOrder->Print();
    // cout<<"Printed sort order after reading"<<endl;
    // fclose(testFile);

    // Testing something end

    // Open the disk file.
    char *b = new char[strlen(f_path) + 1]{};
    copy(f_path, f_path + strlen(f_path), b);
    filepath = b;
    cout<<"File path create"<<filepath<<endl;
    file.Open(0, filepath);
    file.Close();
    return 1;

}

void SortedDBFile::Load (Schema &f_schema, const char *loadpath) {
    if(filepath == NULL || filepath[0] == '\0') {
        cerr<<"First call create() and then load()."<<endl;
        return;
    }
    
    if(dbFileMode == read) {

        OrderMaker &orderRef = *(sortInfo->myOrder);
        pthread_t thread1;
        threadArgs inputArgs = {*input, loadpath, &f_schema};
	    pthread_create (&thread1, NULL, producer, (void*) &inputArgs);
        bigQ = new BigQ(*input, *output,orderRef, sortInfo->runLength);
        dbFileMode = write;

        threadArgs outputArgs = {*output, filepath, NULL};
        pthread_t thread2;
        pthread_create (&thread2, NULL, consumer, (void*) &outputArgs);

        pthread_join (thread1, NULL);
        pthread_join (thread2, NULL);
        // Record rec;
        // off_t numberOfrecords = 0;
        // while(output->Remove(&rec)) {
        //     AddRecordToDiskFile(rec);
        //     numberOfrecords++;
        // }
        // file.AddPage(&page, totalPageCount);
        // page.EmptyItOut();
        // cout<<"Number of records loaded :"<<numberOfrecords<<endl;
    } else {
        // TODO : ask someone what to do in write mode when load is called
    }

    // Record temp;
    // page.EmptyItOut();
    // file.Open(1, filepath);
    // off_t numberOfrecords = 0;
    // while (temp.SuckNextRecord (&f_schema, tableFile)) {
    //     numberOfrecords++;
    //     Add(temp);
    // }
    // file.AddPage(&page, totalPageCount);
    // page.EmptyItOut();
    // cout<<"Number of records loaded :"<<numberOfrecords<<endl;
}

int SortedDBFile::Open (const char *f_path) {
    if(f_path == NULL || f_path[0] == '\0') {
        cerr<<"Empty file path!!!"<<endl;
        return 0;
    }
    // Read metadata from file
    sortInfo = new SortInfo();
    char meta[] = ".metaSorted";
    char *c = new char[strlen(f_path) + 11]{};
    strcpy(c, f_path);
    strcat(c, meta);

    // FILE *metaSortedFile;
    // cout<<"File path :"<<c<<endl;
    // metaSortedFile = fopen(c, "r");
    // sortInfo = new SortInfo();
    // fread(sortInfo, sizeof(struct SortInfo), 1, metaSortedFile);

    // cout<<"Sort Order"<<(char*)sortInfo->myOrder<<endl;
    // sortInfo->myOrder->Print();
    // cout<<"Printed sort order"<<endl;
    ifstream metaSortedFile (c);
    SortInfo *s = new SortInfo();

    if (metaSortedFile.is_open())
    {
        char* line = new char[1000];

        // ignore first line coz it says "sorted", which we already know
       // metaSortedFile.getline(line,1000);

        // next line is runlen (only added in final demo)
        // put it into runlen
        metaSortedFile.getline(line,1000);
        string mystringrunlen (line);
        istringstream bufferrunlen(mystringrunlen);
        bufferrunlen >> s->runLength;

        // next line is number of attributes
        // put it into numAtts
        int numAtts;
        metaSortedFile.getline(line,1000);
        string mystring (line);
        istringstream buffer(mystring);
        buffer >> numAtts;

        // next lines are att numbers and types e.g.
        // 1 String
        // create array to hold our attributes (att nums and types)
        // we will pass this array to OrderMaker.buildOrderMaker
        myAtt* myAtts = new myAtt[numAtts];
        for(int i=0;i<numAtts;i++){
        metaSortedFile.getline(line,1000);
        string mystring (line);
        unsigned found1 = mystring.find_last_of(" ");

        // save att number in array
        string attNo = mystring.substr(0,found1+1);
        istringstream buffer(attNo);
        buffer >> myAtts[i].attNo;

        // save att type in array
        string attType = mystring.substr(found1+1);
        if(attType.compare("Int")==0){
            myAtts[i].attType = Int;
        }
        else if(attType.compare("Double")==0){
            myAtts[i].attType = Double;
        }
        else if(attType.compare("String")==0){
            myAtts[i].attType = String;
        }
        }
        s->myOrder = new OrderMaker();
        s->myOrder->initOrderMaker(numAtts,myAtts);
    }


    sortInfo = s;
    char *path = new char[strlen(f_path) + 1]{};
    copy(f_path, f_path + strlen(f_path), path);
    filepath = path;
    cout<<"Bine file location :"<<f_path<<endl;
    file.Open(1, path);
    return 1;
}

void SortedDBFile::MoveFirst () {
    if(dbFileMode == write) {
        MergeData();
    } 
    page.EmptyItOut();
    //cout<<"Move first"<<endl;
    if(file.GetLength()>0) {
        file.GetPage(&page, 0);
    }
    currentPage = 0;
}
int SortedDBFile::Close () {
    //cout<< "in retrun "<< endl;
    if(dbFileMode == write) {
        MergeData();
    }
    file.Close();
    bigQCreated = false;
    //cout << "returnnnninnnngg"<<endl;
    return 1;
}
// void SortedDBFile::AddRecordToDiskFile(Record &rec) {
//     if (!page.Append(&rec)) {
//         file.AddPage(&page, totalPageCount);
//         totalPageCount++;
//         page.EmptyItOut();
//         if(!page.Append(&rec)) {
//             cout<<"Something wrong happended while adding record to new page";
//             exit(0);
//         }
//     }
// }

void SortedDBFile::Add (Record &rec) {
    dbFileMode = write;
    if(!bigQCreated) {
        bigQCreated= true;
        OrderMaker &orderRef = *(sortInfo->myOrder);
        input = new Pipe(buffsz);
        output = new Pipe(buffsz);
        //bigQ = new BigQ(*input, *output,orderRef, sortInfo->runLength);
        myWorkerUtil* t = new myWorkerUtil();
        t->inputPipe = input;
        t->outputPipe = output;
        t->sortOrder = &orderRef;
        t->runlen = sortInfo->runLength;

        // set up bigQ using a separate thread. See comments in
        // myWorkerRoutine to understand why
        pthread_create(&myWorkerThread, NULL, myWorkerRoutine, (void*)t);
        dbFileMode = write;
    }
    input->Insert(&rec);
}

int SortedDBFile::GetNext (Record &fetchme) {
    if(dbFileMode == write) {
        MergeData();

    }
    totalPageCount = file.GetLength() - 1;
    if (page.GetFirst(&fetchme)) {
        return 1;
    } else {
        if (currentPage + 1 < totalPageCount) {
            currentPage++;
            //cout<<"Current Page :"<<currentPage<<endl;
            file.GetPage(&page, currentPage);
            page.GetFirst(&fetchme);
            return 1;
        } else {
            return 0;
        }
    }
}

int SortedDBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    if(dbFileMode == write) {
        MergeData();
    }
    if(!getNextCalledBefore){
        queryOrder = new OrderMaker();
        //cout<<"Sort Order"<<(char*)sortInfo->myOrder<<endl;
        sortInfo->myOrder->Print();
        //cout<<"Printed sort order"<<endl;
        cnf.createQueryOrder(*sortInfo->myOrder,*queryOrder);

        if(queryOrder->getNumAtts()==0) { // sortOrder and the CNF the user entered had NOTHING in common.
                                        // ****************** OR (since assignment 3) ******************
                                        // the query entered was something like (abc = abc) i.e., both
                                        // sides of the equation are the same attribute. The intended effect
                                        // is to simply return ALL the records in the file.
                                        //
                                        // No point doing a binary search. Start from first record in file
                                        // and peform a linear scan (i.e., plain vanilla heap file search
                                        // behavior).
        // cout << "sorted.getnext no common attrs. using plain getnext" << endl;
        return GetNext(fetchme,cnf,literal);
    }
    else { // sortOrder and the CNF the user entered had something in common.
           // Conduct a binary search to get into the ballpark
      // cout << "sorted.getnext common attrs found. start binary search" << endl;
      if(BinarySearch(fetchme,*sortInfo->myOrder,literal,*queryOrder)){ // binary search found something in the ballpark
        // We must now check that the record that was found also satisfies the CNF entered
        // by the user. Essentially, we're checking that the attributes NOT in the queryOrder
        // order maker that we constructed (but IN the CNF entered by the user) are satisfied
        // for this record.
        getNextCalledBefore = true; // binary search has succeeded once, no need to redo it unless MoveFirst, Add etc are called.
        ComparisonEngine compEngine;
        if(compEngine.Compare (&fetchme, &literal, &cnf)){
          // cout << "sorted.getnext binary search true AND CNF true" << endl;
          return 1;
        }
        else{ // try the next records sequentially
          // cout << "sorted.getnext binary search true BUT CNF false. trying next recs" << endl;
          return GetNext(fetchme,cnf,literal);
        }
      }
      else{ // binary search found nothing even remotely in the ballpark
        // cout << "sorted.getnext binary search failed" << endl;
        return 0;
      }
    }
}
}

void SortedDBFile::MergeData(){
    input->ShutDown();
    dbFileMode = read;
    MoveFirst();
    Record fileRecord, pipeRecord;
    ComparisonEngine ceng;
    File tempFile;
    Page tempPage;
    int pageCount = 0;
    char *mergeFileName = "../bin/mergeFile.bin";
    cout<<mergeFileName<<endl;
    tempFile.Open(0, mergeFileName);
    bool fileEmpty = false;
    bool outputPipeEmpty = false;
    if (GetNext(fileRecord) != 1){
        cout<<"file empty"<<endl;
        fileEmpty = true;
    }
    if (output->Remove(&pipeRecord) != 1){
        cout<<"pipe empty"<<endl;
        outputPipeEmpty = true;
    }
    if(!(fileEmpty | outputPipeEmpty)) {
        cout<<"Begin loop"<<endl;
        while (true) {
            //returns 1 if fileRecord is greater
            if(ceng.Compare (&fileRecord, &pipeRecord, sortInfo->myOrder) == 1) {
                AddRecordToDiskFile(tempFile, tempPage, pipeRecord, pageCount);
                if(output->Remove(&pipeRecord) != 1) {
                    outputPipeEmpty = true;
                    break;
                }
            } else {
                AddRecordToDiskFile(tempFile, tempPage, fileRecord, pageCount);
                if(GetNext(fileRecord) != 1) {
                    fileEmpty = true;
                    break;

                }
            }
            
        }
    }
    int count = 1;
    Schema mySchema("catalog", "nation");
    if(!outputPipeEmpty) {
        AddRecordToDiskFile(tempFile, tempPage, pipeRecord, pageCount);
        while(output->Remove(&pipeRecord)) {
            count ++;
            AddRecordToDiskFile(tempFile, tempPage, pipeRecord, pageCount);
        }
        //outputPipeEmpty = true;
    }
    if(!fileEmpty) {
        AddRecordToDiskFile(tempFile, tempPage, fileRecord, pageCount);
        while(GetNext(fileRecord)) {
            count++;
            //fileRecord.Print(&mySchema);
            AddRecordToDiskFile(tempFile, tempPage, fileRecord, pageCount);
        }
        //fileEmpty = true;
    }
    //cout<<filepath<<endl;
    //cout<<mergeFileName<<endl;
    tempFile.AddPage(&tempPage, pageCount);

    //Now delete the original file and rename our merged file to original file.
    Close();
    //file.Close();
    
    //cout<<mergeFileName<<endl;
    remove(filepath);
    cout<<"File path:"<<filepath<<endl;
    if (rename(mergeFileName, filepath) != 0)
		cout<<"Error moving file"<<endl;
	else
    {
        cout << "File moved successfully"<<endl;
        tempFile.Close(); // TODO: delete the file later
        remove(mergeFileName);
        // Reinitialize the class variables
        file.Open(1, filepath);
        page.EmptyItOut();
        cout<<"New File length :"<<file.GetLength()<<endl;
        //file.GetPage(&page, 0);
        totalPageCount = file.GetLength();
        currentPage = 0;
    }
		
}

void SortedDBFile::AddRecordToDiskFile(File &tempFile, Page &tempPage, Record &rec, int &tempPageCount) {
    if (!tempPage.Append(&rec)) {
        tempFile.AddPage(&tempPage, tempPageCount);
        tempPageCount++;
        tempPage.EmptyItOut();
        if(!tempPage.Append(&rec)) {
            cout<<"Something wrong happended while adding record to new page";
            exit(0);
        }
    }
    //cout<<"Record added"<<endl;
}

int SortedDBFile :: GetNumofRecordPages(){
    int correctLength = file.GetLength();
    if(correctLength!=0) 
        return correctLength - 1;
    else 
        return correctLength;
}


int SortedDBFile :: readOnePage(Record &fetchme){
  //WritePageIfDirty();
  if(page.GetFirst(&fetchme)==0){                // 0 indicates we have read everything from
                                                      // this page. So we can fetch the next page,
                                                      // if there is one.
    int currNumofRecordPages = GetNumofRecordPages(); // this needs to be calculated anew
                                                      // every time because there might have
                                                      // been a write since the last read
                                                      // (if the page was dirty)
    if(currentPage < currNumofRecordPages-1){          // currPageNo numbering starts at 0, hence < and not <=
      currentPage++;
      // cout << "(Heap.cc) GetNext currPageNo = " << currPageNo << " currNumofRecordPages = " << currNumofRecordPages+1 << endl; // diagnostic
      file.GetPage(&page,currentPage); // get a new page in preparation for the next call
      return 0;
    }
    else return 2; // we've gone past the end of the file.
  }
  else
    return 1; // we fetched a record successfully
}

/*------------------------------------------------------------------------------
 * comparison function used by binary search
 *----------------------------------------------------------------------------*/
bool binarySearchCompare (Record* left,Record* right) {
  ComparisonEngine compEngine;
  if(right==pointerToLiteral) // the literal was handed in on the right
    return compEngine.Compare (right, &myQueryOrder, left, &mySortOrder) > 0; // binary_search specs demand that this function return true
                                                                              // if left should precede right.
                                                                              // IMPORTANT! queryOrder must be the second (and NOT
                                                                              // the fourth argument) because it very well may have
                                                                              // fewer attributes than the sortOrder
  else // the literal was handed in on the left
    return compEngine.Compare (left, &myQueryOrder, right, &mySortOrder) < 0; // binary_search specs demand that this function return true if
                                                                              // if left should precede right.
                                                                              // IMPORTANT! queryOrder must be the second (and NOT
                                                                              // the fourth argument) because it very well may have
                                                                              // fewer attributes than the sortOrder
}

/*------------------------------------------------------------------------------
 * added in assignment 2 part 2
 * called by Sorted.GetNext WITH CNF to perform a binary search on sorted's basefile
 * this function will search the whole file if need be.
 * performs a binary search on a page worth of data at a time
 * Returns:
 * true: the element we're looking for was found and the pointer has been positioned
 *       to just after it.
 * false: the element we're looking for was not found in the file
 *----------------------------------------------------------------------------*/
bool SortedDBFile :: BinarySearch(Record& fetchme,OrderMaker& leftOrder,Record& literal,OrderMaker& rightOrder){
  // cout << "heap.binarysearch entered" << endl;
  mySortOrder = leftOrder;
  myQueryOrder = rightOrder;
  pointerToLiteral = &literal; // used in binarySearchCompare to determine which of the arguments is the literal. This changes every
                               // time the function is called because of the way STL binary_search is written.
  ComparisonEngine compEngine;
  vector<Record*> runOfRecs;
  Record* temp = new Record();
  int retval = readOnePage(*temp);
  Schema myschema("catalog", "nation");
  bool done = false;
  while(!done){
    if(retval==2)
      done = true; // 2 signifies the end of the entire file. Nothing left to read

    if(retval==1){ // a record was successfully read but the page has more records.
      runOfRecs.push_back(temp);
      temp = new Record();
      retval = readOnePage(*temp); // initiate the next read
    }
    else{ // retval == 0 signifies that the end of the page was reached i.e., we now have one page worth of records.
          // Time to initiate binary search. If binary search returns true, we need to position our record pointer
          // at the matching element. then, we can linearly search from thereon in GetNext WITH CNF by making
          // subsequent calls to GetNext WITHOUT CNF.
      // cout << "heap.binarysearch page finished with recs " << runOfRecs.size() << endl;
      int compareLiteralAndFront = compEngine.Compare (&literal, &myQueryOrder, runOfRecs.front(), &mySortOrder); // IMPORTANT! queryOrder must be the second (and NOT
                                                                                                                  // the fourth argument) because it very well may have
                                                                                                                  // fewer attributes than the sortOrder
      int compareLiteralAndBack  = compEngine.Compare (&literal, &myQueryOrder, runOfRecs.back(), &mySortOrder); // IMPORTANT! queryOrder must be the second (and NOT
                                                                                                                 // the fourth argument) because it very well may have
                                                                                                                 // fewer attributes than the sortOrder
      // runOfRecs.front()->Print(new Schema("catalog","customer"));
      // runOfRecs.back()->Print(new Schema("catalog","customer"));
      if(compareLiteralAndFront==0){ // 0 indicates that the first element is what we're looking for
        fetchme = *(runOfRecs.front());
        // We need to position the pointer to just after the first (i.e., the matched) record so that 
        // successive calls to GetNext WITHOUT CNF can pick up from there.
        if(!done) currentPage--; // first decrement to undo the increment we made in readOnePage (case for return 0). That increment happens only
                                // if we're not on the last page, hence the check for done not true
        file.GetPage(&page,currentPage); // re-read the page we just read (i.e., the one we found the match in)
        page.GetFirst(&fetchme);
        while(compEngine.Compare (&literal, &myQueryOrder, &fetchme, &mySortOrder)!=0){ // keep searching till you find Waldo
                                                                                        // IMPORTANT! queryOrder must be the second (and NOT
                                                                                        // the fourth argument) because it very well may have
                                                                                        // fewer attributes than the sortOrder
          page.GetFirst(&fetchme);
        }
        return true;
      }
      else if(compareLiteralAndBack==0){ // 0 indicates that the last element is what we're looking for
        fetchme = *(runOfRecs.back());
        // the matching record was the last record on the page. the pointer must point to
        // the start of the NEXT page so successive calls to GetNext WITHOUT CNF can
        // pick up from there. We've already done this in the readOnePage function (case for return 0),
        // so nothing else to do.
        return true;
      }
      else if(compareLiteralAndFront<0){ // the very first element of this page is already bigger than what we're looking for
                                         // hence, the element we're looking for won't be found now or anytime after this
        return false;
      }
      else if(compareLiteralAndBack>0){ // the last element of this page is smaller than what we're looking for.
                                        // no point looking at this page any more. keep looking on successive pages.

        int vecSize = runOfRecs.size(); // empty the vector first so we start afresh
        for(int i=0;i<vecSize;i++){
          delete runOfRecs[i]; // http:// stackoverflow.com/a/4061458
        }
        runOfRecs.erase(runOfRecs.begin(),runOfRecs.end());

        if(!done){ // there are more pages to read
          // cout << "heap.binarysearch page read in next page" << endl;
          temp = new Record();
          retval = readOnePage(*temp); // initiate the next read
        }
        else // we've reached the end of the file
          return 0;
      }
      else if ((compareLiteralAndFront>0) && (compareLiteralAndBack<0)){ // we're in the correct range of records. there is
                                                                          // a chance (it's NOT certain; the record we want
                                                                          // may not even *be* in this file). in any case,
                                                                          // we need to do a binary search to find out.
                                                                          // Note that the only advantage of doing a binary search
                                                                          // IN TERMS OF TIME is that it helps us to very quickly
                                                                          // determine whether the record we want is even in this file.
                                                                          // In the event that it is, however, you *will* need to linearly
                                                                          // scan the page until you get to it so as to position your
                                                                          // pointer correctly for subsequent calls to GetNext WITHOUT
                                                                          // CNF. So you will be taking O(n) to do that anyway. Which means
                                                                          // your binary search didn't really help you to save on that time.
        if(binary_search(runOfRecs.begin(),runOfRecs.end(),&literal,binarySearchCompare)){ // binary search found the record we were looking for
          // We need to position the pointer to just after the matched record so that 
          // successive calls to GetNext WITHOUT CNF can pick up from there.
          // The matched record is somewhere in this page. Look for it.
          if(!done) currentPage--; // first decrement to undo the increment we made in readOnePage (case for return 0). That increment happens only
                                  // if we're not on the last page, hence the check for done not true
          file.GetPage(&page,currentPage); // re-read the page we just read (i.e., the one we found the match in)
          page.GetFirst(&fetchme);
          // fetchme.Print(new Schema("catalog","customer"));
          while(compEngine.Compare (&literal, &myQueryOrder, &fetchme, &mySortOrder)!=0){ // keep searching till you find Waldo
                                                                                          // IMPORTANT! queryOrder must be the second (and NOT
                                                                                          // the fourth argument) because it very well may have
                                                                                          // fewer attributes than the sortOrder
            page.GetFirst(&fetchme);
            // fetchme.Print(new Schema("catalog","customer"));
          }
          return true;
        }
        else // binary search could not find the record we were looking for. because this is the only page our record
             // could've been on (remember the range of the page measured with compareLiteralAndFront and compareLiteralAndBack),
             // no point looking further
          return false;
      }
    }
  }
  if(retval==2) return false;
}

