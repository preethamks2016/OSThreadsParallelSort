#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/time.h>
#include <assert.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <string.h>
#include <sys/sysinfo.h>

typedef struct {
    int key;
    int record[24];
} key_rec_t;

int N; // size of the array
int NUM_THREADS; //number of threads
int SIZE; //numbers per thread
int OFFSET; // offset if array size is not equally divisible by number of threads
key_rec_t* arr; // global array

int compare(const void * a, const void * b) {
    key_rec_t* r1 = (key_rec_t*) a;
    key_rec_t* r2 = (key_rec_t*) b;
    return (r1->key - r2->key);
}

int checkIfFileExists(const char* filename){
    struct stat buffer;
    int exist = stat(filename,&buffer);
    if(exist == 0)
        return 1;
    else  
        return 0;
}

void *sort(void *arg)
{   
    int idx = (long) arg;
    int start = idx * SIZE;
    int size = SIZE;
    if (idx==NUM_THREADS-1) {
        size+=OFFSET;
    }

    //in-built quick sort
    qsort(arr+start, size, sizeof(key_rec_t), compare);
    pthread_exit(NULL);
}

void merge(int l, int r, int mid) {
    int len1 = mid-l, len2 = r-mid;
    key_rec_t* A = (key_rec_t*) malloc (len1* sizeof(key_rec_t));
    key_rec_t* B = (key_rec_t*) malloc (len2* sizeof(key_rec_t));
    //copy left array
    for(int i=0; i<len1; i++) {
        memcpy(&A[i], &arr[l+i], sizeof(key_rec_t));
    }
    //copy right array
    for(int i=0; i<len2; i++) {
        memcpy(&B[i], &arr[mid+i], sizeof(key_rec_t));
    }


    //merge the 2 arrays into the global array
    int i=0,j=0,k=l;
    while (i<len1 && j<len2) {
        if(A[i].key<=B[j].key) {
            memcpy(&arr[k], &A[i], sizeof(key_rec_t));
            i++;
        } else {
            memcpy(&arr[k], &B[j], sizeof(key_rec_t));
            j++;
        }
        k++;
    }

    //copy the remaining elements
    while(i<len1) {
        memcpy(&arr[k], &A[i], sizeof(key_rec_t));
        i++;
        k++;
    }
    while(j<len2) {
        memcpy(&arr[k], &B[j], sizeof(key_rec_t));
        j++;
        k++;
    }
}

// recursive merge
void mergeResults(int numArrays, int scaleFactor, pthread_t threads[]) {
    // printf("merging for numArrays: %d\n", numArrays);
    if (numArrays<1) return;
    for(int i=0;i<numArrays;i+=2) {
        if( scaleFactor == 1) {
            pthread_join(threads[i], NULL);
            pthread_join(threads[i+1], NULL);
        }
        int l = i*SIZE*scaleFactor;
        int r = (i+2)*SIZE*scaleFactor;
        int mid = (l+r)/2;
        if (r>N) r=N; // truncate
        //printf("before merge\n");
        merge(l,r,mid);
    }
    mergeResults(numArrays/2, scaleFactor*2, threads);
    return;
}
   
int main(int argc, char *argv[])
{
    // struct timeval  start, end;
    // double time_spent;

    int fd;
    /* Information about the file. */
    struct stat s;
    int status;
    size_t size;

    /* The file name to open. */
    if ( argc < 3) {
        fprintf(stderr, "%s", "An error has occurred\n");
        exit(0);
    }
    const char * in_file_name = argv[1];
    const char * out_file_name = argv[2];

    /* Open the file for reading. */
    if ( checkIfFileExists(in_file_name) ) {
        fd = open (in_file_name, O_RDONLY);
    }
    else {
        fprintf(stderr, "%s", "An error has occurred\n");
        exit(0);           
    }

    status = fstat (fd, &s);  
    if (status < 0 ) {
        fprintf(stderr, "%s", "An error has occurred\n");
        exit(0);
    }

    size = s.st_size;

    /* Memory-map the file. */
    key_rec_t* arrMap = (key_rec_t*) mmap (0, size, PROT_READ, MAP_PRIVATE, fd, 0);
    if ( arrMap == MAP_FAILED) {
        fprintf(stderr, "%s", "An error has occurred\n");
        exit(0);
    }

    N = size/sizeof(key_rec_t);

    arr = (key_rec_t*) malloc(N*sizeof(key_rec_t));
    // copy to new array
    for(int i=0;i<N;i++) {
        memcpy(&arr[i], &arrMap[i], sizeof(key_rec_t));
    }

    //N=10;
    NUM_THREADS=get_nprocs();
    SIZE = N/NUM_THREADS;
    OFFSET = N%NUM_THREADS;

    pthread_t threads[NUM_THREADS];

    // gettimeofday(&start, NULL); // get start time

    for(int i=0; i<NUM_THREADS; i++) {
        // printf("Thread %d\n", i);
        int ret = pthread_create(&threads[i], NULL, sort, (void *) (size_t) i);
        if (ret!=0) {
            fprintf(stderr, "%s", "An error has occurred\n");
            exit(0);
        }
    }

    // // wait for all threads to complete
    // for(int i=0; i<NUM_THREADS; i++) {
    //     pthread_join(threads[i], NULL);
    // }

    // merge the arrays recursively
    mergeResults(NUM_THREADS, 1, threads);

    int fo = open(out_file_name, O_CREAT | O_WRONLY | O_TRUNC, 0666);
    if ( fo < 0) {
        fprintf(stderr, "%s", "An error has occurred\n");
        exit(0);
    }
    key_rec_t* temp = arr;
    for ( int k = 0 ; k <N ; k++) {
        key_rec_t output_rec;
        memcpy ( &output_rec , temp, sizeof(key_rec_t) );
        int rc = write(fo, &output_rec, sizeof(key_rec_t));

        assert(rc == sizeof(key_rec_t));
        temp++;
    }
    // calculate time
    // gettimeofday(&end, NULL);
    // time_spent = ((double) ((double) (end.tv_usec - start.tv_usec) / 1000000 +
    //                         (double) (end.tv_sec - start.tv_sec)));
    // printf("\nTime taken for execution: %f seconds\n", time_spent);
    fsync(fo);
    exit(0);
}