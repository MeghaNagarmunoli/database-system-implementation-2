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
using namespace std;

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
}

SortedDBFile::~SortedDBFile () {
    //delete(filepath);
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
    FILE *metaSortedFile;
    metaSortedFile = fopen(c, "w");
    fwrite(startup, sizeof(struct SortInfo), 1, metaSortedFile);
    fclose(metaSortedFile);
    sortInfo = (SortInfo*)startup;

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
    FILE *metaSortedFile;
    metaSortedFile = fopen(c, "r");
    fread(sortInfo, sizeof(struct SortInfo), 1, metaSortedFile);

    char *path = new char[strlen(f_path) + 1]{};
    copy(f_path, f_path + strlen(f_path), path);
    filepath = path;
    cout<<f_path<<endl;
    file.Open(1, path);
    //delete(path);
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
    if(dbFileMode == write) {
        MergeData();
    }
    file.Close();
    bigQCreated = false;
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
    // Record temp;
    // totalPageCount = file.GetLength() - 1;
    // while (currentPage < totalPageCount) {
    //     if (page.GetFirst(&temp)) {
    //         if (comparator.Compare(&temp, &literal, &cnf)) {
    //             fetchme.Consume(&temp);
    //             return 1;
    //         }
    //     } else {
    //         if (currentPage + 1 < totalPageCount) {
    //             currentPage++;
    //             file.GetPage(&page, currentPage);
    //         } else {
    //             return 0;
    //         }
    //     }
    // }
    // return 0;
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
        cout<<"writing new record ********* ";
        AddRecordToDiskFile(tempFile, tempPage, pipeRecord, pageCount);
        while(output->Remove(&pipeRecord)) {
            count ++;
            cout<<"writing new record"<<count<<endl;
            AddRecordToDiskFile(tempFile, tempPage, pipeRecord, pageCount);
        }
        //outputPipeEmpty = true;
    }
    if(!fileEmpty) {
        cout<<"writing new record in file empty";
        AddRecordToDiskFile(tempFile, tempPage, fileRecord, pageCount);
        while(GetNext(fileRecord)) {
            count++;
            cout<<"writing new record in file empty"<<count<<endl;
            fileRecord.Print(&mySchema);
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
