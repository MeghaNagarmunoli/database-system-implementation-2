#include "BigQ.h"

void BigQ::createFileWithRuns(Pipe* inputPipe, vector<pair <int,int>> &runMetadata,  int &numRuns, int *runLength, OrderMaker *sort_order, File &new_file) {
	int pageIndex   =  0;		//pageindex to write at
	int globalPageIndex   =  1;
	int globalPageIndexStart = 1;	
	int numRecords  =  0;		//no of records 
	vector <Record*> runVector;
	Page runPage;
	Record *temporary = new Record();
    while(inputPipe->Remove(temporary)){

        Record *copyRecord = new Record;
        copyRecord->Copy(temporary);
        if(runPage.Append(temporary)){
            runVector.push_back(copyRecord);
            numRecords++;
        }
        else if(++pageIndex == *runLength){

            //sort
            sortRun(runVector, new_file, globalPageIndex, sort_order);
            // maintian the metadata for page and run
            runMetadata.push_back(make_pair(globalPageIndexStart,globalPageIndex-1));
            globalPageIndexStart = globalPageIndex;

            // update counters
            numRuns++;
            pageIndex = 0;
            numRecords = 0;
            //empty it
            runVector.clear();
            runPage.EmptyItOut();
            runPage.Append(temporary);
            runVector.push_back(copyRecord);
            numRecords++;
        } else {
            runPage.EmptyItOut();
            runPage.Append(temporary);
            runVector.push_back(copyRecord);
            numRecords++;
        }
    }
    //sort
    sortRun(runVector, new_file, globalPageIndex, sort_order);
    runMetadata.push_back(make_pair(globalPageIndexStart,globalPageIndex-1));
	new_file.Close();
	cout<<"Number of records" << numRecords<<endl;
	delete temporary;
}


void* BigQ::SortAndMerge(void* arg){

	/*Typecast the arguments
	*/
	args_phase1_struct *args;
	args = (args_phase1_struct *)arg;

	//Create new DBFile object
	File new_file;

	char f_path[12] = "runfile.txt";
	new_file.Open(0,f_path);

	
	Page *p = new Page[*(args->run_length)]();	
    Page runPage;
 
	Record *temporary = new Record();//check mem

    vector <Record*> runVector;
	int num_runs  =  1;		//goes from 1 to n,set to one as the array size is n, else set array size to n+1 to use indexing from 1
	vector<pair <int,int>> runMetadata;

    createFileWithRuns(args->input, runMetadata, num_runs, args->run_length,args->sort_order, new_file);

	new_file.Open(1,f_path);

	priority_queue<rwrap* , vector<rwrap*> , sort_func> pQueue (sort_func(args->sort_order));

	//build priority queue

	Page *buf = new Page[num_runs];

	rwrap *temp = new rwrap;
	Record *t = new Record();	

	for (int i=0;i<num_runs;i++){
		
		new_file.GetPage((buf+i),(off_t)(runMetadata[i].first));
		(buf+i)->GetFirst(t);
		(temp->rec).Consume(t);
		temp->run=i;		
		pQueue.push(temp);	
		t = new Record();
		temp = new rwrap;
	}
		
	cout<<"Setting Indexes\n";	

	int flags  = num_runs;
	int next  = 0;
	int start = 0;
	int end   = 1;
	int fin[num_runs];//set to 0
	int c_i[num_runs];	
	int index[num_runs][2];


	for(int i=0;i<num_runs;i++){
		fin[i]=0;
		c_i[i]=0;     
	}
	


    int k=0;
	for(pair<int, int> runPair: runMetadata){

		index[k][start] =  runPair.first;
		index[k][end]   = runPair.second;
        cout<<"Run index:"<<k<<" start -"<<index[k][start]<<" end-"<<index[k][end]<<endl;
        k++;
	}

  
		
	for(int i=0;i<num_runs;i++){

		cout<<"run "<<i<<" start "<<index[i][start]<<" end "<<index[i][end]<<"\n";	
	
	}	

	cout<<"Begin Merge\n";
	while(flags!=0){

		rwrap *temp;
		temp = pQueue.top();
		pQueue.pop();

		next = temp->run;

		args->output->Insert(&(temp->rec));

		rwrap *insert = new rwrap;		
		Record *t = new Record();
 
		if(fin[next]==0)
		{
			if((buf+next)->GetFirst(t)==0){
			
				if( index[next][start]+ (++c_i[next]) > index[next][end]){
				//what do you insert?
					flags--;
					fin[next]=1;				
				}
				else{

					cout<<"Read at index"<<index[next][start]+c_i[next]<<"\n";

					new_file.GetPage(buf+next,(off_t)(index[next][start]+c_i[next]));

					(buf+next)->GetFirst(t);
					insert->rec = *t;
					insert->run = next; 	
					pQueue.push(insert);
					
				}

			}
			else{

				insert->rec = *t;
				insert->run = next; 	
				pQueue.push(insert);

			}	
		}	
	
	}

	while(!pQueue.empty()){
		args->output->Insert(&((pQueue.top())->rec));
		pQueue.pop();
	} 
}


BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
	
	this->no_runs = 0;
	
	args_phase1.input = &in;
	args_phase1.output = &out;
	args_phase1.sort_order = &sortorder;
	args_phase1.run_length = &runlen;
	
	sortorder.Print();
	
	pthread_create (&worker, NULL, &BigQ::SortAndMerge , (void *)&args_phase1);

	pthread_join(worker,NULL);

	out.ShutDown ();
}




BigQ::~BigQ () {

}