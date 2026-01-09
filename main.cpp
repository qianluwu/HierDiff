/**
 * Benchmark driver for evaluating bitmap-based MVCC designs in HexaDB.
 *
 * When the macro Original_HexaDB is enabled, the benchmark uses the original
 * HexaDB bitmap MVCC implementation as the baseline.
 * Otherwise, it evaluates the optimized bitmap controller proposed
 * in this work.
 *
 * The benchmark measures concurrent bitmap insertion and snapshot
 * query performance under multi-threaded workloads.
 */
#include <algorithm>
#include <bitset>
#include <chrono>
#include <cstring>
#include <fstream>
#include <iostream>
#include <list>
#include <mutex>
#include <random>
#include <vector>
#include <atomic>
#include <thread>
#include <condition_variable>

//#define Original_HexaDB
//#define test_memory
//#define Rubbish_Delete

#ifdef Original_HexaDB
    /**
     * Baseline bitmap MVCC controller from the original HexaDB.
     */
    #include "OriginalHexaDBController.h"
#else
    /**
     * Optimized bitmap MVCC controller with hierarchical grouping
     * and differential encoding.
     */
    #include "HierDiffController.h"
#endif

/**
 * A synchronized parallel-for utility.
 *
 * All worker threads are guaranteed to reach the same start point
 * before execution begins, ensuring fair timing measurement for
 * concurrent workloads.
 */
template <class Function>
inline double ParallelForStable(size_t start, size_t end, size_t numThreads, Function fn) {
    if (numThreads <= 0) numThreads = std::thread::hardware_concurrency();
    if (numThreads == 1) {
        auto ts = std::chrono::high_resolution_clock::now();
        for (size_t i = start; i < end; ++i) fn(i, 0);
        auto te = std::chrono::high_resolution_clock::now();
        return std::chrono::duration<double, std::micro>(te - ts).count();
    }

    std::vector<std::thread> threads;
    std::atomic<size_t> current(start);

    std::mutex mtx;
    std::condition_variable cv;
    bool ready = false;
    std::atomic<int> ready_count{0};

    std::exception_ptr lastException = nullptr;
    std::mutex lastExceptMutex;

    for (size_t threadId = 0; threadId < numThreads; ++threadId) {
        threads.emplace_back([&, threadId]{
            ready_count.fetch_add(1, std::memory_order_acq_rel);

            std::unique_lock<std::mutex> lk(mtx);
            cv.wait(lk, [&]{ return ready; });
            lk.unlock();

            while (true) {
                size_t id = current.fetch_add(1);
                if (id >= end) break;
                try {
                    fn(id, threadId);
                } catch (...) {
                    std::unique_lock<std::mutex> le(lastExceptMutex);
                    lastException = std::current_exception();
                    current = end;
                    break;
                }
            }
        });
    }

    while (ready_count.load(std::memory_order_acquire) < (int)numThreads) {
        std::this_thread::yield();
    }

    auto start_tp = std::chrono::high_resolution_clock::now();
    {
        std::lock_guard<std::mutex> lk(mtx);
        ready = true;
    }
    cv.notify_all();

    for (auto &t : threads) t.join();
    auto end_tp = std::chrono::high_resolution_clock::now();

    if (lastException) std::rethrow_exception(lastException);
    return std::chrono::duration<double, std::micro>(end_tp - start_tp).count();
}


/**
 * Bit masks for setting individual bits inside a byte.
 */
std::vector<uint8_t> bitset_vector({1, 2, 4, 8, 16, 32, 64, 128});
const int max_insert = 20;
const int haimin_distence = 1;


/**
 * Randomly flips a small number of bits in a bitmap.
 *
 * This function simulates incremental updates between
 * adjacent bitmap versions.
 */
void RandomSet(uint8_t *bitmap, int num) {
    int max_tryTimes = 200;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> bitmap_dist(0, BITMAP_SIZE - 1);
    std::uniform_int_distribution<> bitset_dist(0, 7);

    for (int i = 0; i < num; i++) {
        bool set_success = false;
        for (int j = 0; j < max_tryTimes; j++) {
            int bitmap_pos = bitmap_dist(gen);
            int bitset_pos = bitset_dist(gen);
            uint8_t result = bitmap[bitmap_pos] | bitset_vector[bitset_pos];
            if (result != bitmap[bitmap_pos]) {
                bitmap[bitmap_pos] = result;
                set_success = true;
                break;
            }
        }

        if (!set_success) {
            throw std::runtime_error("Function Error!! RandomSet() failed!!");
        }
    }
}

/**
 * A reference bitmap used for correctness verification.
 *
 * Each instance stores the full bitmap content corresponding
 * to a specific CSN.
 */
struct InputBitmap {
    int bitmap_csn;
    uint8_t *input_bitmap;
    InputBitmap() : bitmap_csn(0), input_bitmap(nullptr) {}
    InputBitmap(int bitmap_csn, uint8_t *input_bitmap) : bitmap_csn(bitmap_csn), input_bitmap(input_bitmap) {}

    InputBitmap(const InputBitmap &other) {
        bitmap_csn = other.bitmap_csn;
        input_bitmap = new uint8_t[BITMAP_SIZE];
        memcpy(input_bitmap, other.input_bitmap, BITMAP_SIZE);
    }

    InputBitmap &operator=(const InputBitmap &other) {
        if (this != &other) {
            bitmap_csn = other.bitmap_csn;
            if (input_bitmap)
                delete[] input_bitmap;
            input_bitmap = new uint8_t[BITMAP_SIZE];
            memcpy(input_bitmap, other.input_bitmap, BITMAP_SIZE);
        }
        return *this;
    }

    bool operator==(const InputBitmap &other) const { return bitmap_csn == other.bitmap_csn; }

     ~InputBitmap() {
         if (input_bitmap != nullptr) {
             delete[] input_bitmap;
             input_bitmap = nullptr;
         }
     }
};

/**
 * A simplified active-transaction list.
 *
 * The list approximates the set of currently visible snapshots
 * and is used to drive garbage collection behavior.
 */
class Curr_TSN_List {
  private:
#ifdef Rubbish_Delete
    const int max_size = 20;
#else
    const int max_size = max_insert + 100;
#endif

    const int delete_size = 10;

  public:
    std::vector<int> tsn_list;

    Curr_TSN_List() {}
    void insert_new_tsn(int new_tsn) {
        if (tsn_list.size() >= max_size) {
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<> tsn_list_random_delete(0, max_size - 1);
            for (int j = 0; j < delete_size; j++) {
                int erase_pos = tsn_list_random_delete(gen);
                tsn_list.erase(tsn_list.begin() + erase_pos);
            }
        }
        tsn_list.emplace(tsn_list.begin(), new_tsn);
    }

    std::vector<int> get_curr_tsn() { return tsn_list; }
};

/**
 * Executes parallel snapshot queries and verifies correctness
 * against the reference bitmap list.
 */
double Test_bitmap_controller(std::list<InputBitmap> &bitmap_list, Curr_TSN_List &tsn_list,
                              BitmapController &bitmap_controller, int num_query_threads) {
    // std::cout << "======= Start Query =======" << std::endl;
    std::ofstream outFile("error result.txt");
    std::vector<int> curr_tsn = tsn_list.get_curr_tsn();
    std::cout<<"curr_tsn.size(): "<<curr_tsn.size()<<std::endl;
    uint8_t **query_result = new uint8_t *[curr_tsn.size()];
    for (int i = 0; i < curr_tsn.size(); i++) {
        query_result[i]=new uint8_t[BITMAP_SIZE]();
    }
    double duration = ParallelForStable(0, curr_tsn.size(), num_query_threads, [&](size_t row, size_t threadId) {
        bitmap_controller.get_bitmap(curr_tsn[row], query_result[row]);
    });
    for (int i=0; i < curr_tsn.size(); i++) {
        InputBitmap ref(curr_tsn[i], nullptr);
        auto true_bitmap = find(bitmap_list.begin(), bitmap_list.end(), ref);
        if (true_bitmap == bitmap_list.end()) {
            std::cout << "Not Find TSN: " << curr_tsn[i] << std::endl;
            throw;
        }
        if (memcmp(query_result[i], true_bitmap->input_bitmap, BITMAP_SIZE) != 0) {
            std::cout << "Read Error!! CSN: " << curr_tsn[i] << std::endl;
            outFile << "query result: ";
            for (int j = 0; j < BITMAP_SIZE; j++) {
                outFile << std::bitset<8>(query_result[i][j]) << ' ';
            }
            outFile << std::endl;
            outFile << "true result: ";
            for (int j = 0; j < BITMAP_SIZE; j++) {
                outFile << std::bitset<8>(true_bitmap->input_bitmap[j]) << ' ';
            }
            outFile << std::endl;
        } else {
            outFile << "Read Over" << std::endl;
        }
    }
    for (int i = 0; i < curr_tsn.size(); i++) {
        delete[] query_result[i];
    }
    delete[] query_result;
    return curr_tsn.size() / (duration / 1000000.0);
}

double Test_bitmap_controller_no_verify(Curr_TSN_List &tsn_list,
                              BitmapController &bitmap_controller, int num_query_threads) {
    // std::cout << "======= Start Query =======" << std::endl;
    std::ofstream outFile("error result.txt");
    std::vector<int> curr_tsn = tsn_list.get_curr_tsn();
    std::cout<<"curr_tsn.size(): "<<curr_tsn.size()<<std::endl;

    std::vector<std::unique_ptr<uint8_t[]>> scratch(num_query_threads);
    for (int t = 0; t < num_query_threads; ++t) {
        scratch[t] = std::unique_ptr<uint8_t[]>(new uint8_t[BITMAP_SIZE]());
    }

    double duration = ParallelForStable(0, curr_tsn.size(), num_query_threads, [&](size_t row, size_t threadId) {
        uint8_t* buf = scratch[threadId].get();
        bitmap_controller.get_bitmap(curr_tsn[row], buf);
    });

    return curr_tsn.size() / (duration / 1000000.0);
}

void print_tsn_list(Curr_TSN_List tsn_list) {
    std::cout << "TSN list size: " << tsn_list.get_curr_tsn().size() << std::endl;
    std::cout << "[";
    for (int i = 0; i < tsn_list.get_curr_tsn().size(); i++) {
        std::cout << tsn_list.get_curr_tsn()[i] << ',';
    }
    std::cout << "]" << std::endl;
}

int main(int argc, char** argv) {
    int num_insert_threads = 16;
    int num_query_threads = 16;
    if (argc >= 2) {
        num_insert_threads = std::stoi(argv[1]);
    }
    if (argc >= 3) {
        num_query_threads = std::stoi(argv[2]);
    }
    std::cout << "num_insert_threads: " << num_insert_threads << std::endl;
    std::cout << "num_query_threads: " << num_query_threads << std::endl;

    Curr_TSN_List tsn_list;
    BitmapController bitmap_controller(tsn_list.tsn_list);
#ifdef test_memory
    uint8_t *pre_bitmap = new uint8_t[BITMAP_SIZE];
#else
    std::list<InputBitmap> bitmap_list;
#endif
    int now_csn = 0;
    bool first_flag = true;
    for (int i = 0; i < max_insert; i++) {
        InputBitmap new_bitmap;
        new_bitmap.bitmap_csn = now_csn++;
        if (first_flag) {
            new_bitmap.input_bitmap = new uint8_t[BITMAP_SIZE]();
            first_flag = false;
        } else {
            new_bitmap.input_bitmap = new uint8_t[BITMAP_SIZE]();
#ifdef test_memory
            memcpy(new_bitmap.input_bitmap, pre_bitmap, BITMAP_SIZE);
#else
            memcpy(new_bitmap.input_bitmap, bitmap_list.front().input_bitmap, BITMAP_SIZE);
#endif
            RandomSet(new_bitmap.input_bitmap, haimin_distence);
        }
#ifdef test_memory
        memcpy(pre_bitmap, new_bitmap.input_bitmap, BITMAP_SIZE);
#else
        bitmap_list.insert(bitmap_list.begin(), new_bitmap);
#endif
        tsn_list.insert_new_tsn(new_bitmap.bitmap_csn);
    }
    std::mutex csn_lock;
    auto pos = bitmap_list.rbegin();
#ifdef Original_HexaDB
    double duration = ParallelForStable(0, max_insert, num_insert_threads, [&](size_t row, size_t threadId) {
        OneBitmap *bitmap = nullptr;
        csn_lock.lock();
        auto local_pos = pos;
        bitmap_controller.insert_null(local_pos->bitmap_csn, bitmap);
        pos++;
        csn_lock.unlock();
        bitmap_controller.insert_bitmap_content(local_pos->input_bitmap, bitmap);
    });
#else
    BitmapRef *ini_ref = nullptr;
    CompressedBitmap *ini_bitmap = nullptr;
    bitmap_controller.insert_null(pos->bitmap_csn, pos->input_bitmap, ini_ref, ini_bitmap);
    if (ini_ref != nullptr) {
        bitmap_controller.insert_bitmap_content(ini_ref, ini_bitmap, pos->input_bitmap);
    }
    pos++;
    std::mutex query_sum_lock;
    int query_sum = 0;
    double duration = ParallelForStable(0, max_insert - 1, num_insert_threads, [&](size_t row, size_t threadId) {
        if(threadId % 2 == 0){
            BitmapRef *ref = nullptr;
            CompressedBitmap *bitmap = nullptr;
            csn_lock.lock();
            auto local_pos = pos;
            bitmap_controller.insert_null(local_pos->bitmap_csn, local_pos->input_bitmap, ref, bitmap);
            pos++;
            csn_lock.unlock();
            if (ref != nullptr) {
                bitmap_controller.insert_bitmap_content(ref, bitmap, local_pos->input_bitmap);
            }
        }else{
            auto local_pos = pos;
            if(local_pos != bitmap_list.rbegin()){
                local_pos--;
            }
            uint8_t* query_result = new uint8_t[BITMAP_SIZE]();
            bitmap_controller.get_bitmap(local_pos->bitmap_csn, query_result);
            query_sum_lock.lock();
            query_sum ++;
            query_sum_lock.unlock();
        }
    });

#endif
    int insert_throughput = tsn_list.get_curr_tsn().size() / (duration / 1000000.0);
    int query_throughput = query_sum / (duration / 1000000.0);
    std::cout << "insert QPS: " << insert_throughput << " insert/s" << std::endl;
    std::cout << "query QPS: " << query_throughput << " query/s" << std::endl;
#ifndef test_memory
    int throughput = Test_bitmap_controller(bitmap_list, tsn_list, bitmap_controller, num_query_threads);
    // int throughput = Test_bitmap_controller_no_verify(tsn_list, bitmap_controller, num_query_threads);
    std::cout << "query QPS: " << throughput << " query/s" << std::endl;
    std::cout << "tsn final size: " << tsn_list.get_curr_tsn().size() << std::endl;
#endif
    return 0;
}