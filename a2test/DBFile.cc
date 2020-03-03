#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include <iostream>
#include <fstream>
#include <stdio.h>
#include <string.h>
#include "HeapDBFile.h"
#include "SortedDBFile.h"
#include "GenericDBFile.h"
using namespace std;

// stub file .. replace it with your own DBFile.cc

DBFile::DBFile () {
    myInternalDB = NULL;
}

DBFile::~DBFile () {
    delete(myInternalDB);
}

int DBFile::Create (const char *f_path, fType f_type, void *startup) {
    ofstream file;

    char *b = new char[strlen(f_path) + 5]{};
    copy(f_path, f_path + strlen(f_path), b);
    //filepath = b;
    char *meta = ".meta";
    char *c = new char[strlen(f_path) + 6]{};
    strcpy(c, f_path);
    strcat(c, meta);
    cout<<c<<endl;
    file.open(c);
    if(file.is_open()) {
        if(f_type == heap){
            file << "h\n";
            myInternalDB = new HeapDBFile();
        }
        else if (f_type == sorted)
        {
            file << "s\n";
            myInternalDB = new SortedDBFile();
        }
        file.close();
        myInternalDB->Create(f_path, f_type, startup);
    }

    

}

void DBFile::Load (Schema &f_schema, const char *loadpath) {
    myInternalDB->Load(f_schema, loadpath);
}

int DBFile::Open (const char *f_path) {
    if(f_path == NULL || f_path[0] == '\0') {
        cerr<<"Empty file path!!!"<<endl;
        return 0;
    }
    cout<<"DB Open"<<endl;
    if(myInternalDB == NULL) {
        char *b = new char[strlen(f_path) + 5]{};
        copy(f_path, f_path + strlen(f_path), b);
        // Meta data file operation
        char *meta = ".meta";
        char *c = new char[strlen(f_path) + 6]{};
        strcpy(c, f_path);
        strcat(c, meta);
        ifstream file;
        file.open(c);
        char fileType = file.get();
        cout<<"File mode:"<<fileType<<endl;
        if(fileType == 'h') {
            myInternalDB = new HeapDBFile();
        } else if (fileType == 's') {
            myInternalDB = new SortedDBFile();
        } else {
            // TODO: throw error
        }

    }

    // Data file Operation 
    myInternalDB->Open(f_path);
}

void DBFile::MoveFirst () {
    myInternalDB->MoveFirst();
}

int DBFile::Close () {
    return myInternalDB->Close();
}

void DBFile::Add (Record &rec) {
    myInternalDB->Add(rec);
}

int DBFile::GetNext (Record &fetchme) {
    myInternalDB->GetNext(fetchme);
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    return myInternalDB->GetNext(fetchme, cnf, literal);
}