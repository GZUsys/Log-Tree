#ifndef BUCKET_H
#define BUCKET_H

#include <vector>
#include <memory>
#include <atomic>
#include <mutex>
#include <algorithm>
#include <iostream>
#define C_SIZE 1024 * 1024*2 // 2MB




struct Bucket {
    double ratio;
    bool is_free;
    uint64_t ptr;
    uint64_t delete_count;
    uint64_t temp_count;
    
    Bucket *prev ;
    Bucket *next;

    Bucket(double r = 0.0, uint64_t p = 0) 
        : delete_count(0),temp_count(0), ratio(r),is_free(false ), ptr(p), prev(this), next(this) {}
    
    bool isEmpty() const {
        if(!this) return true;
        return prev == this && next == this;
    }
};
void printBucketStats(const std::vector<Bucket*>& buckets);
void headInsert(Bucket *head, Bucket *bucket) {
    if (!head) {
        std::cerr << "Error: headInsert() called with null head pointer\n";
        return; 
    }
    if (!bucket) {
        std::cerr << "Error: headInsert() called with null bucket pointer\n";
        return;
    }
    bucket->next = head->next;
    bucket->prev = head;
    
    if (head->next) {
        head->next->prev = bucket;
    } else {
        head->next = bucket;
        head->prev = bucket;
    }
    
    head->next = bucket;
}

struct Bucket *  getTailbucket(Bucket *head) {
    Bucket *tail = head->prev;
    if (tail == head) {
        return nullptr; // Empty list
    }
    return tail;
}

void removeFromList(Bucket* head, Bucket* bucket) {
    if (!bucket || bucket == head) return; 
    bucket->prev->next = bucket->next;
    bucket->next->prev = bucket->prev;
    bucket->prev = bucket;
    bucket->next = bucket;
}

std::vector<Bucket*> getsumone(std::vector<Bucket*> &buckets,double threshold){
    std::vector<Bucket*> result;
    if (buckets.empty()) {
        return result; 
    }
    int src_index = 10*threshold;
    for(int i =9;i>=src_index;i--){
        if(buckets[i]->isEmpty()) continue;
        struct Bucket *head = buckets[i];
        double sumRatio = 0;
        struct Bucket *temp = head->next;
        while(temp != head){
            if (temp->ratio > threshold) {
                if (std::find(result.begin(), result.end(), temp) != result.end()) continue; // 避免重复
                result.push_back(temp);
                sumRatio += temp->ratio;
            }
            if (sumRatio >= 1) goto out;
            int next_index = 10-10*sumRatio;
            next_index = next_index==0? 1:next_index;
            for (int j = next_index;j<i;j++){
                if (buckets[j]->isEmpty()) continue;
                struct Bucket *mblk = getTailbucket(buckets[j]);
                if (mblk == NULL||mblk->ratio ==0||mblk->delete_count ==0) continue;
                if (std::find(result.begin(), result.end(), mblk) != result.end()) continue; // 避免重复
                result.push_back(mblk);
                goto out;
            }
            temp = temp->next;
        }
    }
    out:
    return result;
    
}



Bucket* getmaxratio(std::vector<Bucket*>& buckets) {
    Bucket* maxBucket = nullptr;
    double maxRatio = -1.0;
    for (Bucket* head : buckets) {
        Bucket* current = head->next;
        while (current != head) {
            if (current->ratio > maxRatio) {
                maxRatio = current->ratio;
                maxBucket = current;
            }
            current = current->next;
        }
    }
    
    return maxBucket; 
}

int getmax(std::vector<Bucket*>&buckets){
    double maxRatio = -1;  
    int maxIndex = -1;           
    for (int i = 0; i < buckets.size(); ++i) {
        if(buckets[i]->isEmpty()) continue; 
        if(buckets[i]->ratio >maxRatio) {
            maxRatio = buckets[i]->ratio;
            maxIndex = i; 
        }
    }
    return maxIndex;
}

int findBucketIndex(double ratio, const std::vector<Bucket*>& buckets) {
    if (ratio < 0.0) return 0;
    if (ratio >= 1.0) return buckets.size() - 1;
    int index = static_cast<int>(ratio * 10);
    if (index < 0) index = 0;
    if (index >= static_cast<int>(buckets.size())) index = buckets.size() - 1;
    
    return index;
}




void locate(std::vector<Bucket*>& buckets, Bucket* bucket) {
    if (!bucket) return; 
    

    int targetIndex = findBucketIndex(bucket->ratio, buckets);
    bool isAlreadyInList = (bucket->prev != bucket) || (bucket->next != bucket);
    
    if (isAlreadyInList) {
        for (Bucket* head : buckets) {
            Bucket* current = head->next;
            while (current != head) {
                if (current == bucket) {
                    removeFromList(head, bucket);
                    break;
                }
                current = current->next;
            }
        }
    }
    
    Bucket* head = buckets[targetIndex];
    headInsert(head, bucket);
}


void reclassifyBuckets(std::vector<Bucket*>& buckets) {
    for (int i = 0; i < buckets.size(); i++) {
        Bucket* head = buckets[i];
        Bucket* current = head->next;
        
        while (current != head) {
            Bucket* nextNode = current->next;
            
            int targetIndex = findBucketIndex(current->ratio, buckets);

            if (targetIndex != i) {
                removeFromList(head, current);
                locate(buckets, current);
            }
            current = nextNode;
        }
    }
}


void printbucket(const std::vector<Bucket*>& buckets) {
    for(size_t i = 0;i<buckets.size();i++){
        Bucket* head = buckets[i];
        Bucket* current = head->next;
        int j =0;
        while(current){
            printf("j is %d,current->delete_count is %lu\n",j,current->delete_count);
            current = current->next;
        }
        printf("----------------------------------------\n");
    }
}


void printBucketStats(const std::vector<Bucket*>& buckets) {
    //using namespace std;
    
    std::cout << "===== Bucket Statistics =====\n";
    
    for (size_t i = 0; i < buckets.size(); ++i) {
        Bucket* head = buckets[i];
        double lowerBound = static_cast<double>(i) / 10.0;
        double upperBound = (i == buckets.size() - 1) ? 
                            std::numeric_limits<double>::max() : 
                            static_cast<double>(i + 1) / 10.0;
        
        std::cout << "Bucket " << i << " (";
        if (i == buckets.size() - 1) {
            std::cout << "1.00~inf): ";
        } else {
            std::cout
                 << lowerBound << "~" << upperBound << "): ";
        }
        if (head->isEmpty()) {
            std::cout << "[EMPTY]\n";
            continue;
        }
        
        Bucket* current = head->next;
        std::vector<double> ratios;
        while (current != head) {
            ratios.push_back(current->ratio);
            current = current->next;
        }
        
        std::sort(ratios.begin(), ratios.end());
        
       
        std::cout << "[";
        for (size_t j = 0; j < ratios.size(); ++j) {
            std::cout << ratios[j];
            if (j < ratios.size() - 1) {
                std::cout << ", ";
                if (j > 0 && j % 5 == 0) std::cout << "\n                   ";
            }
        }
        std::cout << "]\n";
        
        auto [min_it, max_it] = std::minmax_element(ratios.begin(), ratios.end());
        std::cout << "    Items: " << ratios.size() 
             << ", Min: " << *min_it
             << ", Max: " << *max_it
             << std::endl;
    }
    
    std::cout << "=============================\n";
}

#endif // BUCKET_SYSTEM_H