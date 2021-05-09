#include <iostream>
#include <thread>
#include "BQ.hpp"
#include "tracer.h"

using namespace std;

static int THREAD_NUM ;
static int TEST_NUM;
static int TEST_TIME;
static double BATCH_RATIO;
static double WRITE_RATIO;

//static char test_str[9] = "deedbeef";

bool * batch_list;
bool * writelist;
uint64_t *runtimelist;
bool* operationlist;
uint64_t ** opvaluelist;
uint64_t * operationnum;

atomic<int> stopMeasure(0);
uint64_t runner_count;
uint64_t operationnums;
uint64_t g_value;

void concurrent_worker(int tid){
    uint64_t l_value = 0;
    int index = 0;
    Tracer t;
    t.startTime();
    while(stopMeasure.load(memory_order_relaxed) == 0){

        for(size_t i = 0; i < TEST_NUM; i++){
            if(writelist[i]){
                if(operationlist[i]){
                    operationnum[i]++;
                    FutureEnqueue(&opvaluelist[i]);
                }else{
                    operationnum[i]++;
                    Enqueue(&opvaluelist[i]);
                }


            }else{
                if(operationlist[i]){
                    operationnum[i]++;
                    FutureDequeue();
                }else{
                    operationnum[i]++;
                    Dequeue();
                }

            }
        }
        uint64_t tmptruntime = t.fetchTime();
        if(tmptruntime / 1000000 >= TEST_TIME){
            stopMeasure.store(1, memory_order_relaxed);
        }
    }
    runtimelist[tid] = t.getRunTime();
}


int main(int argc, char **argv){
    if (argc == 6) {
        THREAD_NUM = stol(argv[1]);
        TEST_TIME = stol(argv[2]);
        TEST_NUM = stol(argv[3]);
        BATCH_RATIO=stod(argv[4]);
        WRITE_RATIO = stod(argv[5]);

    } else {
        printf("./kv_rw <thread_num>  <test_time> <test_num> <write_ratio>\n");
        return 0;
    }

    cout<<"thread_num "<<THREAD_NUM<<endl<<
        "test_time "<<TEST_TIME<<endl<<
        "test_num "<<TEST_NUM<<endl<<
        "batch_ratio "<<BATCH_RATIO<<
        "write_ratio "<<WRITE_RATIO<<endl;

    //init kvlist
    operationnum = new uint64_t [THREAD_NUM];
    for(size_t i = 0; i < THREAD_NUM ; i++) {
        operationnum[i]=0;
    }

    opvaluelist = new uint64_t *[TEST_NUM];
    for(size_t i = 0;i < TEST_NUM;i++){
        opvaluelist[i] = new uint64_t(i);
    }

    runtimelist = new uint64_t[THREAD_NUM]();


    srand(time(NULL));
    operationlist = new bool[TEST_NUM];
    writelist = new bool[TEST_NUM];
    for(size_t i = 0; i < TEST_NUM; i++ ){
        operationlist[i] = rand() * 1.0 / RAND_MAX * 100  < BATCH_RATIO;
        writelist[i] = rand() *1.0 / RAND_MAX * 100 < WRITE_RATIO;
    }

    vector<thread> threads;
    for(size_t i = 0; i < THREAD_NUM; i++){
        threads.push_back(thread(concurrent_worker,i));
    }
    for(size_t i = 0; i < THREAD_NUM; i++){
        threads[i].join();
    }

    double runtime = 0 , throughput = 0;
    for(size_t i = 0 ; i < THREAD_NUM; i++){
        runtime += runtimelist[i];
        operationnums+=operationnum[i];
    }
    runtime /= THREAD_NUM;
    throughput = operationnums * 1.0 / runtime;
    cout<<"runtime "<<runtime / 1000000<<"s"<<endl;
    cout<<"***throughput "<<throughput<<endl<<endl;

    size_t a1=0,a2=0;
    for(size_t i = 0; i < TEST_NUM;i++){
        if(writelist[i]) a1++;
    }
    cout<<"write_num "<<a1<<endl;
}