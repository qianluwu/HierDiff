#ifndef BITMAP_CONTROLLER_H
#define BITMAP_CONTROLLER_H

#include <assert.h>
#include <cstdint>
#include <mutex>
#include <unistd.h>
#include <utility>
#include <vector>
#include <atomic>
#include <thread>
#include <filesystem>
#include <bitset>
#include <fstream>
#include <iostream>

const int BITMAP_SIZE = 7500;
const int MAX_COMPRESS_NUM = 9;

/**
 * A differential bitmap version.
 * Each node represents one bitmap version with a specific CSN.
 */
struct CompressedBitmap {
    int bitmap_csn;                                      // Commit sequence number
    std::atomic<CompressedBitmap*> next_bitmap;          // Next version in the chain
    uint16_t *compressed_bitmap;                          // Differential or full bitmap
    bool is_compressed;                                   // Compression flag

    CompressedBitmap()
        : bitmap_csn(0), next_bitmap(nullptr),
          compressed_bitmap(nullptr), is_compressed(false) {}
};

/**
 * Reference bitmap (group head).
 * Maintains a complete bitmap and a chain of differential versions.
 */
struct BitmapRef {
    std::mutex ref_lock;                                  // Synchronization for group updates
    int bitmap_cnt;                                       // Number of versions in this group
    std::pair<int, int> csn_range;                         // CSN range covered by this group
    std::atomic<BitmapRef*> next_ref;                      // Next group
    std::atomic<CompressedBitmap*> first_compressed_bitmap;
    uint8_t *complete_bitmap;                              // Reference bitmap

    BitmapRef()
        : bitmap_cnt(0), csn_range(0, 0),
          next_ref(nullptr), first_compressed_bitmap(nullptr),
          complete_bitmap(nullptr) {}
};

/**
 * BitmapController manages multi-version bitmap chains with
 * hierarchical grouped differential encoding.
 */
class BitmapController {
  public:
    BitmapController(std::vector<int>& tsn_list_ref)
        : tsn_list(tsn_list_ref), stop_flag(false)
    {
        head_bitmap_cnt = 9;
    }

    ~BitmapController() {}

    /**
     * Bitwise XOR for bitmap difference computation.
     */
    static void xor_function(uint8_t *a, uint8_t *b, uint8_t *result) {
        for (size_t i = 0; i < BITMAP_SIZE; ++i) {
            result[i] = a[i] ^ b[i];
        }
    }

    /**
     * Union two sorted sparse bitmap arrays.
     */
    static void union_sorted_array(uint16_t*& a, uint16_t* b) {
        int a_size = a[0];
        int b_size = b[0];

        uint16_t* a_data = a + 1;
        uint16_t* b_data = b + 1;

        uint16_t* temp = new uint16_t[a_size + b_size + 1];
        int i = 0, j = 0, k = 1;

        while (i < a_size && j < b_size) {
            if (a_data[i] < b_data[j]) {
                temp[k++] = a_data[i++];
            } else if (a_data[i] > b_data[j]) {
                temp[k++] = b_data[j++];
            } else {
                temp[k++] = a_data[i];
                i++; j++;
            }
        }
        while (i < a_size) temp[k++] = a_data[i++];
        while (j < b_size) temp[k++] = b_data[j++];

        temp[0] = k - 1;

        delete[] a;
        a = new uint16_t[k];
        for (int t = 0; t < k; t++) {
            a[t] = temp[t];
        }
        delete[] temp;
    }

    /**
     * Compress a bitmap version using differential encoding.
     * Dense differences fall back to full bitmap storage.
     */
    uint16_t *compress_bitmap(uint8_t *original_bitmap,
                              uint8_t *complete_bitmap,
                              bool &is_compressed) {
        uint8_t *temp = new uint8_t[BITMAP_SIZE];
        xor_function(original_bitmap, complete_bitmap, temp);

        int total_cnt = 0;
        for (int i = 0; i < BITMAP_SIZE; i++) {
            if (temp[i] != 0) {
                for (int j = 0; j < 8; j++) {
                    if (temp[i] & (1 << (7 - j))) {
                        total_cnt++;
                    }
                }
            }
        }

        if (total_cnt >= BITMAP_SIZE / 16) {
            is_compressed = false;
            uint16_t *compressed_bitmap = new uint16_t[BITMAP_SIZE / 2];
            for (int i = 0; i < BITMAP_SIZE / 2; i++) {
                compressed_bitmap[i] =
                    static_cast<uint16_t>(original_bitmap[2 * i]) |
                    (static_cast<uint16_t>(original_bitmap[2 * i + 1]) << 8);
            }
            return compressed_bitmap;
        }

        is_compressed = true;
        uint16_t *compressed_bitmap = new uint16_t[total_cnt + 1];
        compressed_bitmap[0] = total_cnt;

        int pos = 1;
        for (int i = 0; i < BITMAP_SIZE; i++) {
            if (temp[i] != 0) {
                for (int j = 0; j < 8; j++) {
                    if (temp[i] & (1 << (7 - j))) {
                        compressed_bitmap[pos++] = i * 8 + j;
                    }
                }
            }
        }
        return compressed_bitmap;
    }

    /**
     * Reconstruct a visible bitmap version from reference and differential bitmap.
     */
    void decompress_bitmap(uint8_t *bitmap_result,
                           uint8_t *complete_bitmap,
                           uint16_t *compressed_bitmap,
                           bool is_compressed) {
        memcpy(bitmap_result, complete_bitmap, BITMAP_SIZE);

        if (!is_compressed) {
            for (int i = 0; i < BITMAP_SIZE / 2; i++) {
                bitmap_result[2 * i] ^= compressed_bitmap[i] & 0xFF;
                bitmap_result[2 * i + 1] ^= (compressed_bitmap[i] >> 8) & 0xFF;
            }
        } else {
            int total_cnt = compressed_bitmap[0];
            for (int i = 1; i <= total_cnt; i++) {
                int pos = compressed_bitmap[i];
                int byte_index = pos / 8;
                int bit_index = pos % 8;
                bitmap_result[byte_index] ^= (1 << (7 - bit_index));
            }
        }
    }

    /**
     * Locate and reconstruct a bitmap version visible to a given CSN.
     */
    bool get_bitmap(int require_csn, uint8_t *bitmap_result) {
        BitmapRef *temp_refp = first_ref.load();

        while (temp_refp != nullptr) {
            if (require_csn < temp_refp->csn_range.first) {
                temp_refp = temp_refp->next_ref.load();
            } else if (require_csn > temp_refp->csn_range.second) {
                return false;
            } else {
                break;
            }
        }
        if (temp_refp == nullptr) return false;

        CompressedBitmap *temp_compressed_bitmap =
            temp_refp->first_compressed_bitmap.load();

        while (temp_compressed_bitmap != nullptr) {
            if (require_csn == temp_compressed_bitmap->bitmap_csn) {
                decompress_bitmap(bitmap_result,
                                  temp_refp->complete_bitmap,
                                  temp_compressed_bitmap->compressed_bitmap,
                                  temp_compressed_bitmap->is_compressed);
                return true;
            }
            temp_compressed_bitmap = temp_compressed_bitmap->next_bitmap.load();
        }
        return false;
    }

    /**
     * Stage 1: insert a placeholder bitmap version.
     * The placeholder reserves the correct position in the version chain.
     */
    bool insert_null(int new_csn,
                     uint8_t *original_bitmap,
                     BitmapRef *&ref,
                     CompressedBitmap *&bitmap) {
        bool create_ref = false;
        BitmapRef *now_first_ref = nullptr;

        head_bitmap_cnt_lock.lock();
        if (head_bitmap_cnt == MAX_COMPRESS_NUM) {
            head_bitmap_cnt = 1;
            create_ref = true;
        } else {
            head_bitmap_cnt++;
            now_first_ref = first_ref.load();
        }
        head_bitmap_cnt_lock.unlock();

        if (create_ref) {
            BitmapRef *new_ref = new BitmapRef();
            new_ref->csn_range.first = new_csn;
            new_ref->csn_range.second = new_csn;
            new_ref->bitmap_cnt++;
            new_ref->complete_bitmap = new uint8_t[BITMAP_SIZE];
            memcpy(new_ref->complete_bitmap, original_bitmap, BITMAP_SIZE);

            CompressedBitmap *new_compressed_bitmap = new CompressedBitmap();
            new_compressed_bitmap->bitmap_csn = new_csn;
            new_compressed_bitmap->compressed_bitmap = new uint16_t[1];
            new_compressed_bitmap->compressed_bitmap[0] = 0;
            new_compressed_bitmap->is_compressed = true;

            new_ref->first_compressed_bitmap.store(new_compressed_bitmap);

            head_lock.lock();
            new_ref->next_ref = first_ref.load();
            first_ref.store(new_ref);
            head_lock.unlock();

            return true;
        }

        CompressedBitmap *new_compressed_bitmap = new CompressedBitmap();
        new_compressed_bitmap->bitmap_csn = new_csn;

        now_first_ref->ref_lock.lock();
        new_compressed_bitmap->next_bitmap =
            now_first_ref->first_compressed_bitmap.load();
        now_first_ref->first_compressed_bitmap.store(new_compressed_bitmap);
        now_first_ref->ref_lock.unlock();

        ref = now_first_ref;
        bitmap = new_compressed_bitmap;
        return true;
    }

    /**
     * Stage 2 & 3: fill placeholder and update visibility range.
     */
    bool insert_bitmap_content(BitmapRef *ref,
                               CompressedBitmap *bitmap,
                               uint8_t *original_bitmap) {
        bitmap->compressed_bitmap =
            compress_bitmap(original_bitmap,
                            ref->complete_bitmap,
                            bitmap->is_compressed);

        ref->ref_lock.lock();
        ref->bitmap_cnt++;

        CompressedBitmap *temp_bmp = ref->first_compressed_bitmap.load();
        CompressedBitmap *start_compress_point = nullptr;
        int temp_csn = -1;

        while (temp_bmp != nullptr && temp_bmp != bitmap) {
            if (temp_bmp->compressed_bitmap == nullptr) {
                temp_csn = -1;
                start_compress_point = nullptr;
            } else {
                temp_csn = temp_bmp->bitmap_csn;
                start_compress_point = temp_bmp;
            }
            temp_bmp = temp_bmp->next_bitmap.load();
        }

        if (start_compress_point != nullptr) {
            temp_bmp = start_compress_point;
            while (temp_bmp != nullptr && temp_bmp != bitmap) {
                union_sorted_array(temp_bmp->compressed_bitmap,
                                   bitmap->compressed_bitmap);
                temp_bmp = temp_bmp->next_bitmap.load();
            }
        }

        if (temp_csn == -1) {
            temp_csn = bitmap->bitmap_csn;
        }
        ref->csn_range.second =
            std::max(ref->csn_range.second, temp_csn);

        ref->ref_lock.unlock();
        return true;
    }

  private:
    std::atomic<BitmapRef*> first_ref = nullptr;   // Head of reference chain
    std::mutex head_lock;
    std::mutex head_bitmap_cnt_lock;
    int head_bitmap_cnt = 0;

    std::thread worker;
    std::vector<int>& tsn_list;
    std::atomic<bool> stop_flag;
    std::mutex mtx;

    void delete_middle_ref(BitmapRef *ref_front,
                           BitmapRef *temp_ref_back) {
        BitmapRef *del_ref = nullptr;
        while (temp_ref_back != ref_front && temp_ref_back != nullptr) {
            del_ref = temp_ref_back;
            temp_ref_back = temp_ref_back->next_ref.load();
            delete del_ref;
        }
    }

    void delete_middle_bitmap(CompressedBitmap *bitmap_front,
                              CompressedBitmap *temp_bmp_back) {
        CompressedBitmap *del_bmp = nullptr;
        while (temp_bmp_back != bitmap_front && temp_bmp_back != nullptr) {
            del_bmp = temp_bmp_back;
            temp_bmp_back = temp_bmp_back->next_bitmap.load();
            delete del_bmp;
        }
    }

    bool merge_group() {
        return true;
    }
};

#endif // BITMAP_CONTROLLER_H
