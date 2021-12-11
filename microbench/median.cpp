#include <cstdlib>
#include <unistd.h>
#include <iostream>
#include <cstdint>
#include <cstring>
#include <atomic>
#include <ctime>
#include <algorithm>

using namespace std;

static constexpr size_t entry_num = 4;
static constexpr size_t _size = 512;
//static constexpr size_t size = 4096;

struct entry_t{
    uint64_t key;
    uint64_t value;
};

struct bucket_t{
    atomic<int> token;
    entry_t entry[entry_num];
};

static size_t bucket_num = _size/sizeof(bucket_t);

void insert4split(bucket_t* bucket, uint64_t key, uint64_t value){
    for(int i=0; i<bucket_num; i++){
	for(int j=0; j<entry_num; j++){
	    if(bucket[i].entry[j].key == 0){
		bucket[i].entry[j].key = key;
		bucket[i].entry[j].value = value;
		return;
	    }
	}
    }
}

bucket_t* split_with_sort(bucket_t* left, uint64_t& split_key){
    entry_t temp[bucket_num*entry_num];
    int idx = 0;
    for(int i=0; i<bucket_num; i++){
	for(int j=0; j<entry_num; j++){
	    temp[idx].key = left[i].entry[j].key;
	    temp[idx].value = left[i].entry[j].value;
	    idx++;
	}
    }
    memset(left, 0x0, sizeof(bucket_t)*bucket_num);

    std::sort(temp, temp+idx, [](entry_t& a, entry_t& b){
	    return a.key < b.key;
	    });

    int half = idx/2;
    split_key = temp[half-1].key;
    auto right = new bucket_t[bucket_num];
    std::cout << "median: " << split_key << std::endl;

    for(int i=0; i<half; i++){
	insert4split(left, temp[i].key, temp[i].value);
    }
    for(int i=half; i<idx; i++){
	insert4split(right, temp[i].key, temp[i].value);
    }
    return right;
}


void swap(entry_t* a, entry_t* b){
    entry_t temp;
    memcpy(&temp, a, sizeof(entry_t));
    memcpy(a, b, sizeof(entry_t));
    memcpy(b, &temp, sizeof(entry_t));
}

int partition(entry_t* entry, int left, int right){
    uint64_t last = entry[right].key;
    int i=left;
    int j=left;
    while(j < right){
	if(entry[j].key < last){
	    swap(&entry[i], &entry[j]);
	    i++;
	}
	j++;
    }
    swap(&entry[i], &entry[right]);
    return i;
}

int random_partition(entry_t* entry, int left, int right){
    int n = right - left + 1;
    int pivot = rand() % n;
    swap(&entry[left+pivot], &entry[right]);
    return partition(entry, left, right);
}

void median_util(entry_t* entry, int left, int right, int k, int& a, int& b){
    if(left <= right){
	int partition_idx = random_partition(entry, left, right);
	if(partition_idx == k){
	    //b = entry[partition_idx].key;
	    b = partition_idx;
	    if(a != -1)
		return;
	}
	else if(partition_idx == k-1){
	    a = partition_idx;
	    //a = entry[partition_idx].key;
	    if(b != -1)
		return;
	}

	if(partition_idx >= k){
	    return median_util(entry, left, partition_idx-1, k, a, b);
	}
	else{
	    return median_util(entry, partition_idx+1, right, k, a, b);
	}
    }
}

int find_median(entry_t* entry, int n){
    int ans;
    int a = -1, b = -1;
    if(n % 2 == 1){
	median_util(entry, 0, n-1, n/2, a, b);
	ans = b;
    }
    else{
	median_util(entry, 0, n-1, n/2, a, b);
	ans = (a+b)/2;
    }
    return ans;
}

bucket_t* split_without_sort(bucket_t* left, uint64_t& split_key){
    entry_t temp[bucket_num*entry_num];
    int idx = 0;
    for(int i=0; i<bucket_num; i++){
	for(int j=0; j<entry_num; j++){
	    temp[idx].key = left[i].entry[j].key;
	    temp[idx].value = left[i].entry[j].value;
	    idx++;
	}
    }
    memset(left, 0x0, sizeof(bucket_t)*bucket_num);
    auto median_idx = find_median(temp, idx);
    split_key = temp[median_idx].key;
    std::cout << "median: " << split_key << std::endl;
    
    auto right = new bucket_t[bucket_num];

    for(int i=0; i<idx; i++){
	if(temp[i].key <= split_key)
	    insert4split(left, temp[i].key, temp[i].value);
	else
	    insert4split(right, temp[i].key, temp[i].value);
    }
    return right;
}

int main(){
    struct timespec start, end;
    auto bucket = new bucket_t[bucket_num];
    auto _bucket = new bucket_t[bucket_num];

    for(int i=0; i<bucket_num; i++){
	for(int j=0; j<entry_num; j++){
	    bucket[i].entry[j].key = rand();
	    bucket[i].entry[j].value = 1;
	}
    }
    memcpy(_bucket, bucket, sizeof(bucket_t)*bucket_num);

    /*
    for(int i=0; i<bucket_num; i++){
	for(int j=0; j<entry_num; j++){
	    std::cout << bucket[i].entry[j].key << " ";
	}
	std::cout << "\n";
    }*/

    {
	clock_gettime(CLOCK_MONOTONIC, &start);
	uint64_t key;
	auto right = split_with_sort(bucket, key);
	clock_gettime(CLOCK_MONOTONIC, &end);
	auto elapsed = (end.tv_nsec - start.tv_nsec) + (end.tv_sec - start.tv_sec)*1000000000;

	std::cout << elapsed << " (nsec) " << std::endl;
	std::cout << elapsed/1000.0 << " (usec) " << std::endl;
	std::cout << elapsed/1000000.0 << " (msec) " << std::endl;
    }
    {
	clock_gettime(CLOCK_MONOTONIC, &start);
	uint64_t key;
	//auto right = split_with_sort(_bucket, key);
	auto right = split_without_sort(_bucket, key);
	clock_gettime(CLOCK_MONOTONIC, &end);
	auto elapsed = (end.tv_nsec - start.tv_nsec) + (end.tv_sec - start.tv_sec)*1000000000;

	std::cout << elapsed << " (nsec) " << std::endl;
	std::cout << elapsed/1000.0 << " (usec) " << std::endl;
	std::cout << elapsed/1000000.0 << " (msec) " << std::endl;
    }


    return 0;
}
