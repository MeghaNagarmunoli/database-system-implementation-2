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
    dbFileMode = readMode;
    bigQ = NULL;
    recordCursor = new Record();
    buffsz = 100;
    Pipe input(buffsz);
    Pipe output(buffsz);
    bigQCreated  =false;
    getNextCalledBefore = false;
    queryOM = new OrderMaker();
    pageIndex=0;
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
    
    if(dbFileMode == readMode) {

        OrderMaker &orderRef = *(sortInfo->myOrder);
        pthread_t thread1;
        threadArgs inputArgs = {*input, loadpath, &f_schema};
	    pthread_create (&thread1, NULL, producer, (void*) &inputArgs);
        bigQ = new BigQ(*input, *output,orderRef, sortInfo->runLength);
        dbFileMode = writeMode;

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
    file.Open(1, path);
    cout<<"Bin file location :"<<f_path<<endl;
    return 1;
}

void SortedDBFile::MoveFirst () {
    if(dbFileMode == writeMode) {
        MergeData();
    } 
    page.EmptyItOut();
    //cout<<"Move first"<<endl;
    if(file.GetLength()>0) {
        file.GetPage(&page, 0);
    }
    currentPage = 0;
    dirtyRead = true;
    pageIndex = 0;
}
int SortedDBFile::Close () {
    //cout<< "in retrun "<< endl;
    if(dbFileMode == writeMode) {
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
    //cout<<"add"<<endl;
    dbFileMode = writeMode;
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
        dbFileMode = writeMode;
    }
    input->Insert(&rec);
}

int SortedDBFile::GetNext (Record &fetchme) {
    if(dbFileMode == writeMode) {
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
    Schema mySch("catalog","nation");
    //recordCursor->Print(&mySch);
    dirtyRead = true;
    if(dbFileMode == writeMode) {
        MergeData();
    }
    OrderMaker dummy, cnfOM;
    cnf.GetSortOrders(cnfOM, dummy);
    for (int i = 0; i < cnfOM.numAtts; i++)
    {
        cnfOM.whichAtts[i] = i;
    }

    if (recordCursor == NULL)
    {
        return FAILURE;
    }
    //OrderMaker queryOM;
    OrderMaker *orderMaker = sortInfo -> myOrder;
    ComparisonEngine ce;
    if(!getNextCalledBefore) {
        page.GetFirst(recordCursor);
        getNextCalledBefore = true;
        int orderNum = 0, queryOrderNum = 0, i = 0;
        for (orderNum = 0; orderNum < orderMaker->numAtts; orderNum++)
        {
            for (i = 0; i < cnf.numAnds; i++)
            {

            if (cnf.orList[i][0].op != Equals)
                continue;

            if (cnf.orLens[i] != 1)
                continue;

            if (cnf.orList[i][0].operand1 == Left && cnf.orList[i][0].operand2 == Literal && orderMaker->whichAtts[orderNum] == cnf.orList[i][0].whichAtt1)
            {

                queryOM->whichAtts[queryOrderNum] = orderMaker->whichAtts[orderNum];
                queryOM->whichTypes[queryOrderNum] = orderMaker->whichTypes[orderNum];
                queryOM->numAtts++;
                break;
            }
            }

            if (i == cnf.numAnds)
            break;
        }
    

        

        int sizeFile = file.GetLength() - 2;

        int m = (sizeFile + pageIndex) / 2, 
        left = pageIndex + 1, 
        right = sizeFile;

        bool found_record = false;
        cout<<"Left :"<<left<<" , Right : "<<right<<endl;
        if (queryOM->numAtts == 0)
        {
            found_record = true;
            ComparisonEngine ce;
            while (!ce.Compare(recordCursor, &literal, &cnf))
            {
                get_next_Record();
                if (recordCursor == NULL)
                    return FAILURE;
            }

            fetchme.Consume(recordCursor);
            get_next_Record();
            dirtyRead = true;
            return SUCCESS;
        }

        else
        {
            if (ce.Compare(recordCursor, queryOM, &literal, &cnfOM) == 0)
            {
                found_record = true;
            }

            while (!found_record && page.GetFirst(recordCursor))
            {
                //recordCursor->Print(&mySch);
                if (ce.Compare(recordCursor, queryOM, &literal, &cnfOM) == 0)
                {
                    found_record = true;
                    break;
                }
            }
            
            while (!found_record && ((right - left) >= 1))
            {
                cout<<"Left :"<<left<<" , Right : "<<right<<endl;
                m = (left + right) / 2;
                if(m != left) {

                }
                file.GetPage(&page, m);
                page.GetFirst(recordCursor);

                if (ce.Compare(recordCursor, queryOM, &literal, &cnfOM) > 0)
                {
                    right = m - 1;
                }

                else if (ce.Compare(recordCursor, queryOM, &literal, &cnfOM) == 0)
                {
                    right = m;
                }

                else if (ce.Compare(recordCursor, queryOM, &literal, &cnfOM) < 0)
                {
                    left = m;
                }
            }

            if (!found_record && left < sizeFile)
            {
                cout<<"Left :"<<left<<endl;
                file.GetPage(&page, left);
                page.GetFirst(recordCursor);

                while (ce.Compare(recordCursor, queryOM, &literal, &cnfOM) != 0)
                {
                    get_next_Record();
                }
            }
        }
    }

    while (ce.Compare(recordCursor, queryOM, &literal, &cnfOM) == 0)
    {
        if (ce.Compare(recordCursor, &literal, &cnf) == SUCCESS)
        {
            fetchme.Consume(recordCursor);
            get_next_Record();
            dirtyRead = true;
            return SUCCESS;
        }

        else
        {
            get_next_Record();

            if (recordCursor == NULL)
            {
                dirtyRead = true;
                return FAILURE;
            }
        }
    }

    dirtyRead = true;
    return FAILURE;
}

void SortedDBFile::MergeData(){
    input->ShutDown();
    int count = 0;
    dbFileMode = readMode;
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
    //cout<<"Begin reading current file"<<endl;
    if (GetNext(fileRecord) != 1){
        cout<<"file empty"<<endl;
        fileEmpty = true;
    }
    if (output->Remove(&pipeRecord) != 1){
        cout<<"pipe empty"<<endl;
        outputPipeEmpty = true;
    }
    if(!fileEmpty && !outputPipeEmpty) {
        cout<<"Begin loop"<<endl;
        while (true) {
            //returns 1 if fileRecord is greater
            if(ceng.Compare (&fileRecord, &pipeRecord, sortInfo->myOrder) == 1) {
                count ++;

                AddRecordToDiskFile(tempFile, tempPage, pipeRecord, pageCount);
                if(output->Remove(&pipeRecord) != 1) {
                    outputPipeEmpty = true;
                    break;
                }
            } else {
                AddRecordToDiskFile(tempFile, tempPage, fileRecord, pageCount);
                count ++;
                if(GetNext(fileRecord) != 1) {
                    fileEmpty = true;
                    break;

                }
            }
            
        }
    }
    //int count = 1;
    Schema mySchema("catalog", "nation");
    if(!outputPipeEmpty) {
        count ++;
        AddRecordToDiskFile(tempFile, tempPage, pipeRecord, pageCount);
        while(output->Remove(&pipeRecord)) {
            count ++;
            AddRecordToDiskFile(tempFile, tempPage, pipeRecord, pageCount);
        }
        //outputPipeEmpty = true;
    }
    if(!fileEmpty) {
        AddRecordToDiskFile(tempFile, tempPage, fileRecord, pageCount);
        count ++;
        while(GetNext(fileRecord)) {
            count++;
            //fileRecord.Print(&mySchema);
            AddRecordToDiskFile(tempFile, tempPage, fileRecord, pageCount);
        }
        //fileEmpty = true;
    }
    //cout<<filepath<<endl;
    cout<<"Records removec count "<<count<<endl;
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

void SortedDBFile::get_next_Record()
{
  // Attempt to get next record
  if (page.GetFirst(recordCursor) == FAILURE)
  {
    // when failed,  checking if the next page exists
    if (pageIndex + 1 == file.GetLength() - 1)
    {
      recordCursor = NULL;
    }

    else
    {
      // It does, load it and the first record from it
      file.GetPage(&page, ++pageIndex);
      page.GetFirst(recordCursor);
    }
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