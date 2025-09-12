#include "log_tree.h"
#include "zipfian.h"
#include "zipfian_util.h"
#include <cmath>
//#include "cpucounters.h" 
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <limits.h>
#include <signal.h>
#include <sys/mman.h>
#include <sys/time.h>
#include <vector>
#include <string.h>
#include <stdint.h>
#include <string>
#include <thread>
#include <atomic>
#include <queue>
#include <immintrin.h>
#include <random>
#include <ctime>
#include <sched.h>
#include <pthread.h>
#include  "pcm.h"
extern "C"
{
    #include <atomic_ops.h>
}  

typedef uint64_t            setkey_t;
typedef void*               setval_t;


#define DEFAULT_DURATION                5000
#define DEFAULT_INITIAL                 100
#define DEFAULT_NB_THREADS              1
#define DEFAULT_RANGE                   0x7FFFFFFF
#define DEFAULT_SEED                    0
#define DEFAULT_UPDATE                  100
#define DEFAULT_ALTERNATE               0
#define DEFAULT_EFFECTIVcpucounters  0 
#define DEFAULT_UNBALANCED              0
//  #define PPCM



#define XSTR(s)                         STR(s)
#define STR(s)                          #s

#define VAL_MIN                         INT_MIN
#define VAL_MAX                         INT_MAX
#define DETECT_LATENCY
//#define UNIFORM
#define test_num   1000000
#define test_thread 128
int initial =     DEFAULT_INITIAL; 
unsigned int levelmax;
uint64_t record[test_thread][test_num]={0};
uint64_t records[50000000]={0};
uint64_t latency[test_thread]={0}, insert_nbs=0 ,insert_nb[test_thread] ={0};
__thread struct timespec T1[test_thread], T2[test_thread];
std::vector<uint64_t> buffer;
std::vector<uint64_t> sbuffer;
std::vector<long> slen;
std::vector<char> ops;
long sfencenum=0;
long flushnum=0;
long dram_alloc_num=0;
long pm_alloc_num=0;

#define BYTES_TO_MB(bytes) ((double)bytes / 1048576.0)
#define BYTES_TO_GB(bytes) ((double)bytes / 1073741824.0)
std::vector<std::vector<uint64_t>>i_vec(128);
std::vector<std::vector<uint64_t>>r_vec(128);
std::vector<std::vector<uint64_t>>d_vec(128);
std::vector<std::vector<uint64_t>>s_vec(128);

struct op {
    uint64_t key;
    long  len;
};


void read_data_from_file(char* file)
{
    long count = 0;

    FILE* fp = fopen(file, "r");
    if (fp == NULL) {
        exit(-1);
    }
    buffer.clear();
    //printf("reading\n");
    while (1) {
        unsigned long long key;
        count = fscanf(fp, "%lld\n", &key);
        if (count != 1) {
            break;
        }
        buffer.push_back(key);
    }
    fclose(fp);
    //printf("file closed\n");
}


void scan_data_from_file(char* file)
{
    long count = 0;

    FILE* fp = fopen(file, "r");
    if (fp == NULL) {
        exit(-1);
    }
    buffer.clear();
    ops.clear();
   // printf("reading\n");
    while (1) {
        char str[100];
	char * p;
        count = fscanf(fp, "%s\n",str);
        if (count != 1) {
            break;
        }
	p=strtok(str,","); 
        buffer.push_back(atoll(p));
	p=strtok(NULL,",");
        ops.push_back(p[0]);
    }
    fclose(fp);
    //printf("file closed\n");
}

void sd_data_from_file(char* file)
{
    long count = 0;

    FILE* fp = fopen(file, "r");
    if (fp == NULL) {
        exit(-1);
    }
    buffer.clear();
    slen.clear();
    //printf("reading\n");
    while (1) {
        char str[100];
        char * p;
        count = fscanf(fp, "%s\n",str);
        if (count != 1) {
            break;
        }
        p=strtok(str,",");
        buffer.push_back(atoll(p));
        p=strtok(NULL,",");
        slen.push_back(atol(p));
    }
    fclose(fp);
   // printf("file closed\n");
}






int main(int argc, char **argv)
{
	struct option long_options[] = {
        // These options don't set a flag
        {"help",                      no_argument,       NULL, 'h'},
        {"duration",                  required_argument, NULL, 'd'},
        {"initial-size",              required_argument, NULL, 'i'},
        {"thread-num",                required_argument, NULL, 't'},
        {"range",                     required_argument, NULL, 'r'},
        {"seed",                      required_argument, NULL, 'S'},
        {"update-rate",               required_argument, NULL, 'u'},
        {"unbalance",                 required_argument, NULL, 'U'},
        {"elasticity",                required_argument, NULL, 'x'},
        {"operation",                 required_argument, NULL, 'o'},
        {NULL,                         0,                 NULL, 0  }
    };

    int i,d;
    long nb_threads;
    char operation;
    int m,s;
    while(1) {
        i = 0;
        d = getopt_long(argc, argv, "hAf:d:i:t:r:S:u:U:c:o:m:n:", long_options, &i);
        if(d == -1) break;
        if(d == 0 && long_options[i].flag == 0) d = long_options[i].val;
        switch(d) {
                case 0:
                    break;
                case 't':
                    nb_threads = atoi(optarg);
                    break;
                case 'o':
                    operation = optarg[0];
                    break;
                case 'm':
                    m =atoi(optarg);
                    break;
                case 'n':
                    s = atoi(optarg);
                    break;
                default:
                    exit(1);
        }
    }
    initBuckets();
    printf("Nb threads   : %d\n",  nb_threads);
    printf("page size is %d\n",sizeof(page));
    printf("header size is %d\n",sizeof(meta));
    printf("entry size is %d\n",sizeof(entry));
    #ifdef USE_PMDK
    openPmemobjPool();
    #ifdef DDM
    mlog =  get_mlog();
    initmm();
    #endif 
    #else
    printf("without pmdk!\n");
    #endif
    char loading_file[100];
    //sprintf(loading_file, "%s", "/home/yzz/tool/gen_key/100M.txt");
    sprintf(loading_file, "%s", "/home/yzz/tool/datafile/baseload.txt");
    printf("load name is %s\n",loading_file);
    read_data_from_file(loading_file);
    memset(record, 0, sizeof(record));
    btree *bt;
    bt = new btree();
    initial=buffer.size();
    struct timeval start_time, end_time,mid_time,sys_start_time,sys_end_time;
    long retus;
    double rets ;
    uint64_t     time_interval;
    isPtr = false;

    struct Tif  Threads[nb_threads];
    uint64_t min_key= MAX_KEY;
    printf("prev insert\n");
    for (i=0;i<nb_threads;i++){
        struct log *new_log = (struct log *)alloc(sizeof(struct log));
        Threads[i].index = 0;
        Threads[i].t_log = new_log;
        Threads[i].wb_index = 1;
        Threads[i].min_key = min_key;
        Threads[i].max_key = 0 ;
    }
    printf("start warm up\n");
    for (int i =0;i<1;i++){
        struct Tif *t = &Threads[i];
        Threads[i].td = std::thread([=](){
            for (uint64_t k =0;k<initial;k++){
                bt->insert(buffer[k],(char *)(buffer[k]),t);
            }
        });
    }
    for (int i = 0; i < 1; ++i) {
        if (Threads[i].td.joinable()) {
            Threads[i].td.join();
        }
    }
   
    


    #ifdef DDMS
    double k[5] = {0.4,0.3,0.25,0.2,0.15};
    // double A15 = 0.15;//(0.85)
    // double A2 = 0.2;//(0.8)
    // double A25 = 0.25;//(0.75)
    // double A30 = 0.3; //(0.7)
    // double A35 = 0.35; //(0.65) 
    // double A4 = 0.4;
    printf("start DDMS\n");
    printf("m is %d,ka is %lf,sleep time is %d\n",m,k[m],s);
    std::thread T2(TN_merge,bt,k[m],s); 
    //T2.detach();
    #endif 

    
    #ifdef PPCM
    pcm::PCM *pcm = pcm::PCM::getInstance();
    pcm->program();
    pcm::SystemCounterState before_state = pcm->getSystemCounterState();
    #endif 
    
    
    switch (operation)
    {

    case 'i': //insert 
        {
            sprintf(loading_file, "%s", "/home/yzz/tool/datafile/insert.txt");
            printf("start insert\n");
            read_data_from_file(loading_file);
            initial = buffer.size();
            //printf("3\n");
            uint64_t step = initial/nb_threads;
            uint64_t all_lat = 0;
            gettimeofday(&start_time, NULL);
            for (int i=0;i<nb_threads;i++){
                struct Tif *t=&Threads[i];
                Threads[i].td = std::thread([=](){
                    uint64_t start = i*step;
                    uint64_t end = (i==nb_threads-1)?(initial-1):(i+1)*step -1;
                    for(uint64_t kk = start;kk<=end;kk++){
                        uint64_t num = buffer[kk];
                        bt->insert(num,(char *)num,t);
                    }
                });
            }
            for (int i = 0; i < nb_threads; ++i) {
                if (Threads[i].td.joinable()) {
                    Threads[i].td.join();
                }
            }
            gettimeofday(&end_time, NULL);
            time_interval = 1000000 * (end_time.tv_sec - start_time.tv_sec) + end_time.tv_usec - start_time.tv_usec;
            printf("insert all k-v time is %ld ns\n",time_interval*1000);
            printf("insert avg time is %lu ns\n",time_interval*1000/initial);  
            printf("MOPS:%.4lf\n",((double)initial)/((double)time_interval));
        }
        break;
    
    case 'r':  //read 
        {
            printf("read\n");
            //sprintf(loading_file, "%s", "/home/yzz/load/read50m.csv");
            sprintf(loading_file, "%s", "/home/yzz/tool/datafile/read.txt");
            read_data_from_file(loading_file);
            initial = buffer.size();
            int step = initial/nb_threads;
            gettimeofday(&start_time, NULL);
            for (int i =0;i<nb_threads;i++){
                struct Tif *t = &Threads[i];
                Threads[i].td = std::thread([=](){
                    uint64_t latency;
                    uint64_t start = i*step;
                    uint64_t end = (i==nb_threads-1)?(initial-1):(i+1)*step -1;
                    char *en = NULL;
                    char **e=&en;
                    for (;start<=end;start++){
                        bt->search(buffer[start],e);
                    }
                });
            }
            for (int i = 0; i < nb_threads; ++i) {
                if (Threads[i].td.joinable()) {
                    Threads[i].td.join();
                }
            }
            gettimeofday(&end_time, NULL);
            time_interval = 1000000 * (end_time.tv_sec - start_time.tv_sec) + end_time.tv_usec - start_time.tv_usec;
            printf("read all k-v time is %ld ns\n",time_interval*1000);
            printf("read avg time is %lu ns\n",time_interval*1000/initial);  
            printf("MOPS:%.4lf\n",((double)initial)/((double)time_interval));
        }
        break;

    case 'd': //delete
        {
            sprintf(loading_file, "%s", "/home/yzz/tool/datafile/delete.txt");
            read_data_from_file(loading_file);
            initial = buffer.size();
            uint64_t step = initial /nb_threads;
            printf("start delete\n");
            gettimeofday(&start_time, NULL);
            for (int i =0;i<nb_threads;i++){
                struct Tif *t = &Threads[i];
                Threads[i].td = std::thread([=](){
                    uint64_t start = i*step;
                    uint64_t end =  (i==nb_threads-1)?(initial-1):(i+1)*step -1;
                    for(uint64_t kk = start;kk<=end;kk++){
                        uint64_t num = buffer[kk];
                        bt->remove(num);
                    }
                });
            }
            for (int i = 0; i < nb_threads; ++i) {
                if (Threads[i].td.joinable()) {
                    Threads[i].td.join();
                }
            }
            gettimeofday(&end_time, NULL);
            time_interval = 1000000 * (end_time.tv_sec - start_time.tv_sec) + end_time.tv_usec - start_time.tv_usec;
            printf("delete all k-v time is %ld ns,%d s\n",time_interval*1000,time_interval/1000000);
            printf("delete avg time is %lu ns\n",time_interval*1000/initial);  //操作一个的平均时长
            printf("MOPS:%.4lf\n",((double)initial)/((double)time_interval));
        }

        break;
 
    case 's':  
        {
            sprintf(loading_file, "%s", "/home/yzz/tool/datafile/scan.txt");
            sd_data_from_file(loading_file);
            initial = buffer.size();
            int step = initial/nb_threads;
            printf("scan start\n");
            gettimeofday(&start_time, NULL);
            for (int i =0;i<nb_threads;i++){
                struct Tif *t = &Threads[i];
                Threads[i].td = std::thread([=](){
                    uint64_t latency;
                    uint64_t start = i*step;
                    uint64_t end = (i==nb_threads-1)?(initial-1):(i+1)*step -1;
                    std::vector<uint64_t>buf(1024);
                    for (;start<=end;start++){
                        bt->scan(buffer[start],slen[start],buf);
                    }
                });
            }
            for (int i = 0; i < nb_threads; ++i) {
                if (Threads[i].td.joinable()) {
                    Threads[i].td.join();
                }
            }
            gettimeofday(&end_time, NULL);
            printf("scan all k-v time is %ld ns\n",time_interval*1000);
            printf("scan avg time is %lu ns\n",time_interval*1000/initial);  
            printf("MOPS:%.4lf\n",((double)initial)/((double)time_interval));
        }

        break;
    default:
        printf("error\n");
        break;
    }

    gettimeofday(&sys_end_time, NULL);
    printf("sys run time is :%lu\n",sys_end_time.tv_sec - sys_start_time.tv_sec);
    printf("struct size log is %lu\n",sizeof(struct log));
    
    #ifdef PPCM
    pcm::SystemCounterState after_state = pcm->getSystemCounterState();
   // printf("L3 misses:%lld\n", pcm::getL3CacheMisses(before_state, after_state));
    printf("L3 misses: %lld\n", pcm::getL3CacheMisses(before_state, after_state));

    printf("DRAM Reads (bytes): %lld (%.2f MB / %.2f GB)\n",
        pcm::getBytesReadFromMC(before_state, after_state),
        BYTES_TO_MB(pcm::getBytesReadFromMC(before_state, after_state)),
        BYTES_TO_GB(pcm::getBytesReadFromMC(before_state, after_state)));

    printf("DRAM Writes (bytes): %lld (%.2f MB / %.2f GB)\n",
        pcm::getBytesWrittenToMC(before_state, after_state),
        BYTES_TO_MB(pcm::getBytesWrittenToMC(before_state, after_state)),
        BYTES_TO_GB(pcm::getBytesWrittenToMC(before_state, after_state)));

    printf("PM Reads (bytes): %lld (%.2f MB / %.2f GB)\n",
        pcm::getBytesReadFromPMM(before_state, after_state),
        BYTES_TO_MB(pcm::getBytesReadFromPMM(before_state, after_state)),
        BYTES_TO_GB(pcm::getBytesReadFromPMM(before_state, after_state)));

    printf("PM Writes (bytes): %lld (%.2f MB / %.2f GB)\n",
        pcm::getBytesWrittenToPMM(before_state, after_state),
        BYTES_TO_MB(pcm::getBytesWrittenToPMM(before_state, after_state)),
        BYTES_TO_GB(pcm::getBytesWrittenToPMM(before_state, after_state)));
    pcm->cleanup();
    printf("load time is flush is %lld\n",flushnum);
    printf("load time is sfence is %lld\n",sfencenum);
    printf("load time is dram_alloc_num is %lld\n",dram_alloc_num);
    printf("load time is pm_alloc_num is %lld\n",pm_alloc_num);
    #endif 

    stop_merge = true;
    #ifdef DDMS
        if(T2.joinable()){
        T2.join();
    }
    #endif

    #ifdef DDMS
    cal_temp();
    #endif 
    cal_space2();
    
    return 0;
}


