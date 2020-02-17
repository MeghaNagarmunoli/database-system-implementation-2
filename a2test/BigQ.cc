#include "BigQ.h"
#include <algorithm>

void BigQ::sortRun(vector<Record*> &records,File& new_file,int& gp_index,OrderMaker *sort_order){
	
    sort(records.begin(),records.end(),sort_func(sort_order));
	int c=0;

	Page *tp = new Page();
    for(Record *record : records) {
		if(tp->Append(record)==0){
			new_file.AddPage(tp,(off_t)(gp_index++));
			tp->EmptyItOut();
			tp->Append(record);
			c++;	
		}
		else{
			c++;
		}						
	}

	new_file.AddPage(tp,(off_t)(gp_index++));	
	cout<<"G index end "<<gp_index<<"\n";
	delete tp;
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
//	cout << "Created run file "<<*(args->num_runs)<<"\n";
	
	int result    =  1;		//integer boolean for checking pipe status
	int num_recs  =  0;		//no of records 
	int p_index   =  0;		//pageindex to write at
	int gp_index   =  1;		//global page index for file
	//int r_index[*(args->run_length)];	
	int num_runs  =  1;		//goes from 1 to n,set to one as the array size is n, else set array size to n+1 to use indexing from 1
	//r_index[num_runs-1]=1;
    int gp_index_start = 1;
    int numPagesWrittenToFile = 0;
	vector<pair <int,int>> runMetadata;

    while(args->input->Remove(temporary)){

        Record *copyRecord = new Record;
        copyRecord->Copy(temporary);
        if(runPage.Append(temporary)){
            runVector.push_back(copyRecord);
             num_recs++;
        }
        else if(++p_index == *(args->run_length)){

            //sort
            sortRun(runVector, new_file, gp_index, args->sort_order);
            // maintian the metadata for page and run
            runMetadata.push_back(make_pair(gp_index_start,gp_index-1));
            gp_index_start = gp_index;



            // update counters
            num_runs++;
            p_index = 0;
            num_recs = 0;
            //empty it
            runVector.clear();
            runPage.EmptyItOut();
            runPage.Append(temporary);
            runVector.push_back(copyRecord);
            num_recs++;
        } else {
            runPage.EmptyItOut();
            runPage.Append(temporary);
            runVector.push_back(copyRecord);
             num_recs++;
        }
    }
      //sort
    sortRun(runVector, new_file, gp_index, args->sort_order);
    runMetadata.push_back(make_pair(gp_index_start,gp_index-1));


	delete temporary;

	new_file.Close();
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