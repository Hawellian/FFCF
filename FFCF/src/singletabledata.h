#ifndef CUCKOO_FILTER_SINGLE_TABLE_DATA_H_
#define CUCKOO_FILTER_SINGLE_TABLE_DATA_H_

#include <assert.h>

#include <sstream>

#include "bitsutil.h"
#include "debug.h"
#include "printutil.h"

namespace cuckoofilter {

// the most naive table implementation: one huge bit array
template <size_t bits_per_data>
class SingleTableData {
  static const size_t kTagsPerBucket = 4;
  static const size_t kBytesPerBucket =
      (bits_per_data * kTagsPerBucket + 7) >> 3;
  static const uint64_t kTagMask = (1ULL << bits_per_data) - 1;
  static const size_t kPaddingBuckets =
      ((((kBytesPerBucket + 7) / 8) * 8) - 1) / kBytesPerBucket;

  struct Bucket {
    char bits_[kBytesPerBucket];
  } __attribute__((__packed__));

  // using a pointer adds one more indirection
  Bucket *buckets_;
  size_t num_buckets_;

 public:
  explicit SingleTableData(const size_t num) : num_buckets_(num) {
    buckets_ = new Bucket[num_buckets_ + kPaddingBuckets];
    memset(buckets_, 0, kBytesPerBucket * (num_buckets_ + kPaddingBuckets));
  }

  ~SingleTableData() { delete[] buckets_; }

  size_t NumBuckets() const { return num_buckets_; }

  size_t SizeInBytes() const { return kBytesPerBucket * num_buckets_; }

  size_t SizeInTags() const { return kTagsPerBucket * num_buckets_; }

  std::string Info() const {
    std::stringstream ss;
    ss << "SingleHashtable with data size: " << bits_per_data << " bits \n";
    ss << "\t\tAssociativity: " << kTagsPerBucket << "\n";
    ss << "\t\tTotal # of rows: " << num_buckets_ << "\n";
    ss << "\t\tTotal # slots: " << SizeInTags() << "\n";
    return ss.str();
  }

  inline uint64_t ReadTag(const size_t i, const size_t j) const {
    const char *p = buckets_[i].bits_;
    uint64_t tag;
    /* following code only works for little-endian */
    if (bits_per_data == 64) {
      tag = ((uint64_t *)p)[j];
    }

    return tag & kTagMask;
  }

  // write tag to pos(i,j)
  inline void WriteTag(const size_t i, const size_t j, const uint64_t t) {
    char *p = buckets_[i].bits_;
    uint64_t tag = t & kTagMask;
    /* following code only works for little-endian */
    if (bits_per_data == 64) {
      ((uint64_t *)p)[j] = tag;
    }
  }

  inline bool DeleteTagFromBucket(const size_t i, const size_t j) {
    if (ReadTag(i, j) != 0) {
      WriteTag(i, j, 0);
      return true;
    }
    return false;
  }

  inline bool InsertTagToBucket(const size_t i, const size_t j,
                                const uint64_t tag, const bool kickout,
                                uint64_t &oldtag) {
    if (ReadTag(i, j) == 0) {
      WriteTag(i, j, tag);
      return true;
    }

    if (kickout) {
      oldtag = ReadTag(i, j);
      WriteTag(i, j, tag);
    }
    return false;
  }

  inline size_t NumTagsInBucket(const size_t i) const {
    size_t num = 0;
    for (size_t j = 0; j < kTagsPerBucket; j++) {
      if (ReadTag(i, j) != 0) {
        num++;
      }
    }
    return num;
  }
};
}  // namespace cuckoofilter
#endif  // CUCKOO_FILTER_SINGLE_TABLE_H_
