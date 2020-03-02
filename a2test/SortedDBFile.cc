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
    FILE *metaSortedFile;
    metaSortedFile = fopen(c, "w");
    fwrite(startup, sizeof(struct SortInfo), 1, metaSortedFile);
    fclose(metaSortedFile);
    sortInfo = (SortInfo*)startup;

    // Open the disk file.
    char *b = new char[strlen(f_path) + 1]{};
    copy(f_path, f_path + strlen(f_path), b);
    filepath = b;
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
    cout<<f_path<<endl;
    file.Open(1, path);
    delete(path);
    return 1;
}

void SortedDBFile::MoveFirst () {
    if(dbFileMode == write) {
        mergeData();
    } 
    page.EmptyItOut();
    file.GetPage(&page, 0);
    currentPage = 0;
}
int SortedDBFile::Close () {
    // file.Close();
    // return 1;
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
    if(dbFileMode == read) {
        OrderMaker &orderRef = *(sortInfo->myOrder);
        input = new Pipe(buffsz);
        output = new Pipe(buffsz);
        bigQ = new BigQ(*input, *output,orderRef, sortInfo->runLength);
        dbFileMode = write;
    }
    input->Insert(&rec);
   
    // if (!page.Append(&rec)) {
    //     file.AddPage(&page, totalPageCount);
    //     totalPageCount++;
    //     page.EmptyItOut();
    //     if(!page.Append(&rec)) {
    //         cout<<"Something wrong happended while adding record to new page";
    //         exit(0);
    //     }
    // }

}




// void DBFile::HopefullyHarmless() {
//     cout<<"Page count :"<<totalPageCount<<endl;
//     file.AddPage(&page, totalPageCount);
//     page.EmptyItOut();
// }

int SortedDBFile::GetNext (Record &fetchme) {
    if(dbFileMode == read) {
        mergeData();
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

void SortedDBFile::mergeData(){
    input->ShutDown();
    dbFileMode = read;
    MoveFirst();
    Record temp1, temp2;
    ComparisonEngine ceng;
    File tempFile;
    Page tempPage;
    int pageCount = 0;
    char *mergeFileName = "../bin/mergeFile.bin";
    tempFile.Open(0, mergeFileName);
    bool fileEmpty = false;
    bool outputPipeEmpty = false;
    if(GetNext(temp1) == 1 & output->Remove(&temp2) == 1 ) {
        while (true) {
            //returns 1 if temp1 is greater
            if(ceng.Compare (&temp1, &temp2, sortInfo->myOrder) == 1) {
                AddRecordToDiskFile(tempFile, tempPage, temp2, pageCount);
                if(output->Remove(&temp2) != 1) {
                    outputPipeEmpty = true;
                    break;
                }
            } else {
                AddRecordToDiskFile(tempFile, tempPage, temp1, pageCount);
                if(GetNext(temp1) != 1) {
                    fileEmpty = true;
                    break;

                }
            }
            
        }
    }
    if(fileEmpty) {
        while(output->Remove(&temp2)) {
            AddRecordToDiskFile(tempFile, tempPage, temp2, pageCount);
        }
        outputPipeEmpty = true;
    }
    if(outputPipeEmpty) {
        while(GetNext(temp1)) {
            AddRecordToDiskFile(tempFile, tempPage, temp1, pageCount);
        }
        fileEmpty = true;
    }

    tempFile.AddPage(&tempPage, totalPageCount);

    //Now delete the original file and rename our merged file to original file.
    if (rename(filepath, mergeFileName) != 0)
		cerr<<"Error moving file"<<endl;
	else
    {
        delete(mergeFileName);
        cout << "File moved successfully"<<endl;
    }
    // Reinitialize the class variables
    file.Open(1, filepath);
    page.EmptyItOut();
    file.GetPage(&page, 0);
    totalPageCount = pageCount;
    currentPage = 0;
		
}

void AddRecordToDiskFile(File &tempFile, Page &tempPage, Record &rec, int &tempPageCount) {
    if (!tempPage.Append(&rec)) {
        tempFile.AddPage(&tempPage, tempPageCount);
        tempPageCount++;
        tempPage.EmptyItOut();
        if(!tempPage.Append(&rec)) {
            cout<<"Something wrong happended while adding record to new page";
            exit(0);
        }
    }
}
