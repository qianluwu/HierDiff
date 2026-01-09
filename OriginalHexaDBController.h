#ifndef BITMAPCONTROLLER_H
#define BITMAPCONTROLLER_H

#include <cstdint>
#include <string.h>
#include <atomic>
#include <vector>
#include <thread>
#include <chrono>
#include <shared_mutex>

#endif // BITMAPCONTROLLER_H

const int BITMAP_SIZE = 7500;

/**
 * A full bitmap version node in HexaDB.
 * Each version stores a complete bitmap and is linked in descending CSN order.
 */
struct OneBitmap {
    OneBitmap *next_bitmap;                    // Next older bitmap version
    int bitmap_csn;                            // Commit sequence number
    uint8_t bitmap_content[BITMAP_SIZE];       // Full bitmap content

    OneBitmap()
        : next_bitmap(nullptr),
          bitmap_csn(0),
          bitmap_content() {}
};

/**
 * BitmapController implements the baseline bitmap-based MVCC scheme in HexaDB.
 * It maintains a single-level version chain with full bitmap copies.
 */
class BitmapController {
public:
    BitmapController(std::vector<int>& tsn_list_ref)
        : tsn_list(tsn_list_ref), stop_flag(false) {}

    ~BitmapController() {
        OneBitmap* p = first_bitmap;
        while (p) {
            OneBitmap* next = p->next_bitmap;
            delete p;
            p = next;
        }
    }

    /**
     * Insert a placeholder bitmap version into the version chain.
     * The actual bitmap content is filled later.
     */
    bool insert_null(int new_csn, OneBitmap *&bitmap) {
        chain_lock.lock();

        OneBitmap *new_bitmap = new OneBitmap();
        new_bitmap->bitmap_csn = new_csn;
        new_bitmap->next_bitmap = first_bitmap;
        first_bitmap = new_bitmap;

        chain_lock.unlock();

        bitmap = new_bitmap;
        return true;
    }

    /**
     * Fill the bitmap content of an already inserted version.
     * This stores a full bitmap snapshot.
     */
    bool insert_bitmap_content(uint8_t *new_content, OneBitmap *bitmap) {
        memcpy(bitmap->bitmap_content, new_content, BITMAP_SIZE);
        return true;
    }

    /**
     * Retrieve the bitmap version visible to a given CSN.
     * The first version whose CSN is not greater than require_csn is returned.
     */
    bool get_bitmap(int require_csn, uint8_t *require_content) {
        chain_lock.lock_shared();

        OneBitmap *tempp = first_bitmap;
        while (tempp != nullptr) {
            if (tempp->bitmap_csn <= require_csn) {
                break;
            }
            tempp = tempp->next_bitmap;
        }

        if (tempp != nullptr) {
            memcpy(require_content, tempp->bitmap_content, BITMAP_SIZE);
            chain_lock.unlock_shared();
            return true;
        } else {
            chain_lock.unlock_shared();
            return false;
        }
    }

private:
    OneBitmap *first_bitmap = nullptr;          // Head of the bitmap version chain
    std::shared_mutex chain_lock;               // Reader-writer lock for version chain

    std::thread worker;
    std::vector<int>& tsn_list;                 // Active transaction CSNs
    std::atomic<bool> stop_flag;

    /**
     * Garbage collection based on the oldest active transaction policy.
     * Versions newer than the oldest active CSN are preserved.
     */
    void delete_bitmap() {
        while (!stop_flag) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));

            chain_lock.lock();

            OneBitmap *tempp = first_bitmap;
            if (tempp == nullptr) return;

            int oldest_tsn = tsn_list[tsn_list.size() - 1];

            while (tempp != nullptr && tempp->bitmap_csn > oldest_tsn) {
                tempp = tempp->next_bitmap;
            }

            OneBitmap *del_bmp = nullptr;
            while (tempp != nullptr) {
                del_bmp = tempp;
                tempp = tempp->next_bitmap;
                delete del_bmp;
            }

            chain_lock.unlock();
        }
    }
};
