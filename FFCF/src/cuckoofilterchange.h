#ifndef CUCKOO_FILTER_CUCKOO_FILTER_CHANGE_H_
#define CUCKOO_FILTER_CUCKOO_FILTER_CHANGE_H_

#include <assert.h>
#include <algorithm>

#include "debug.h"
#include "hashutil.h"
#include "packedtable.h"
#include "printutil.h"
#include "singletablewithencode.h"

namespace cuckoofilter {
// status returned by a cuckoo filter operation
enum Status {
  Ok = 0,
  NotFound = 1,
  NotEnoughSpace = 2,
  NotSupported = 3,
};

// maximum number of cuckoo kicks before claiming failure
const size_t kMaxCuckooCount = 500;

template <typename ItemType, size_t bits_per_item,
          template <size_t> class TableType = SingleTableWithEncode,
          typename HashFamily = TwoIndependentMultiplyShift>
class CuckooFilterChangeFLength {
  // Storage of items
  TableType<bits_per_item> *table_;
  size_t num_items_;

  typedef struct {
    size_t index;
    uint32_t tag;
    bool used;
  } VictimCache;

  VictimCache victim_;

  HashFamily hasher_;

  inline size_t IndexHash(uint32_t hv) const {
    return hv & (table_->NumBuckets() - 1);
  }

  inline uint32_t TagHash(uint32_t hv) const {
    uint32_t tag;
    tag = hv & ((1ULL << (2 * bits_per_item)) - 1);
    tag += (tag == 0);
    return tag;
  }

  inline void GenerateIndexTagHash(const ItemType &item, size_t *index,
                                   uint32_t *tag) const {
    const uint64_t hash = hasher_(item);
    *index = IndexHash(hash >> 32);
    *tag = TagHash(hash);
  }

  inline size_t AltIndex(const size_t index, const uint32_t tag) const {
    return IndexHash((uint32_t)(index ^ (tag * 0x5bd1e995)));
  }

  Status AddImpl(const size_t i, const uint32_t tag, const ItemType &item);

  /**
   * @brief
   *
   * @param i
   * @param tag
   * @return Status
   */
  Status AddImplWithFN(const size_t i, const uint32_t tag,
                       const size_t var_kMaxCuckooCount, const ItemType &item);

  double LoadFactor() const { return 1.0 * Size() / table_->SizeInTags(); }
  double BitsPerItem() const { return 8.0 * table_->SizeInBytes() / Size(); }

 public:
  explicit CuckooFilterChangeFLength(const size_t max_num_keys)
      : num_items_(0), victim_(), hasher_() {
    size_t assoc = 4;
    size_t num_buckets = 8192;
    double frac = (double)max_num_keys / num_buckets / assoc;
    if (frac > 0.96) {
      std::cout << "CF might fail." << std::endl;
    }
    victim_.used = false;
    table_ = new TableType<bits_per_item>(num_buckets);
  }

  ~CuckooFilterChangeFLength() { delete table_; }

  // Add an item to the filter.
  Status Add(const ItemType &item);

  // Add an item to the filter.
  Status AddWithFN(const ItemType &item, const size_t var_kMaxCuckooCount = 16);

  // Report if the item is inserted, with false positive rate.
  Status Contain(const ItemType &item) const;

  Status ChangeFingerprint(const ItemType &item);
  // Delete an key from the filter
  Status Delete(const ItemType &item);

  /* methods for providing stats  */
  // summary infomation
  std::string Info() const;

  size_t Size() const { return num_items_; }
  size_t SizeInBytes() const { return table_->SizeInBytes(); }
  size_t SBucketInfo(int i) const { return table_->BucketInfo(i); }
};

template <typename ItemType, size_t bits_per_item,
          template <size_t> class TableType, typename HashFamily>
Status CuckooFilterChangeFLength<ItemType, bits_per_item, TableType,
                                 HashFamily>::Add(const ItemType &item) {
  size_t i;
  uint32_t tag;

  if (victim_.used) {
    std::cout << std::string(80, '=') << std::endl;
    if (Contain(item) == Ok) {
      std::cout << "Item was already in the set." << std::endl;
    } else {
      std::cout << "Item was not already in the set." << std::endl;
    }
    std::cout << "Info: ";
    std::cout << Info();
    std::cout << std::string(80, '=') << std::endl;
    return NotEnoughSpace;
  }

  GenerateIndexTagHash(item, &i, &tag);
  return AddImpl(i, tag, item);
}

template <typename ItemType, size_t bits_per_item,
          template <size_t> class TableType, typename HashFamily>
Status CuckooFilterChangeFLength<
    ItemType, bits_per_item, TableType,
    HashFamily>::AddWithFN(const ItemType &item,
                           const size_t var_kMaxCuckooCount) {
  size_t i;
  uint32_t tag;

  GenerateIndexTagHash(item, &i, &tag);
  return AddImpl(i, tag, item);
}

template <typename ItemType, size_t bits_per_item,
          template <size_t> class TableType, typename HashFamily>
Status CuckooFilterChangeFLength<ItemType, bits_per_item, TableType,
                                 HashFamily>::AddImpl(const size_t i,
                                                      const uint32_t tag,
                                                      const ItemType &item) {
  size_t curindex = i;
  size_t curindexnomeans;
  uint32_t curtag = tag;
  uint32_t oldtag;
  uint64_t curitem = item;
  uint64_t olditem;

  for (uint32_t count = 0; count < kMaxCuckooCount; count++) {
    bool kickout = count > 0;
    oldtag = 0;
    olditem = 0;
    if (table_->InsertTagToBucket(curindex, curtag, kickout, oldtag, curitem,
                                  olditem)) {
      num_items_++;
      return Ok;
    }
    if (kickout) {
      curitem = olditem;
      GenerateIndexTagHash(curitem, &curindexnomeans, &curtag);
    }
    curindex = AltIndex(curindex, curtag);
  }

  victim_.index = curindex;
  victim_.tag = curtag;
  victim_.used = true;
  return Ok;
}

template <typename ItemType, size_t bits_per_item,
          template <size_t> class TableType, typename HashFamily>
Status CuckooFilterChangeFLength<
    ItemType, bits_per_item, TableType,
    HashFamily>::AddImplWithFN(const size_t i, const uint32_t tag,
                               const size_t var_kMaxCuckooCount,
                               const ItemType &item) {
  size_t curindex = i;
  uint32_t curtag = tag;
  uint32_t oldtag;
  uint64_t curitem = item;
  uint64_t olditem;

  for (uint32_t count = 0; count < kMaxCuckooCount; count++) {
    bool kickout = count > 0;
    oldtag = 0;
    if (table_->InsertTagToBucket(curindex, curtag, kickout, oldtag, curitem,
                                  olditem)) {
      num_items_++;
      return Ok;
    }
    if (kickout) {
      curtag = oldtag;
    }
    curindex = AltIndex(curindex, curtag);
  }
  return Ok;
}

template <typename ItemType, size_t bits_per_item,
          template <size_t> class TableType, typename HashFamily>
Status CuckooFilterChangeFLength<ItemType, bits_per_item, TableType,
                                 HashFamily>::Contain(const ItemType &key)
    const {
  bool found = false;
  size_t i1, i2;
  uint32_t tag;

  GenerateIndexTagHash(key, &i1, &tag);
  i2 = AltIndex(i1, tag);

  assert(i1 == AltIndex(i2, tag));

  found = victim_.used && (tag == victim_.tag) &&
          (i1 == victim_.index || i2 == victim_.index);

  if (found || table_->FindTagInBuckets(i1, i2, tag)) {
    return Ok;
  } else {
    return NotFound;
  }
}

template <typename ItemType, size_t bits_per_item,
          template <size_t> class TableType, typename HashFamily>
Status
CuckooFilterChangeFLength<ItemType, bits_per_item, TableType,
                          HashFamily>::ChangeFingerprint(const ItemType &key) {
  size_t i1, i2;
  uint32_t tag;

  GenerateIndexTagHash(key, &i1, &tag);
  i2 = AltIndex(i1, tag);
  assert(i1 == AltIndex(i2, tag));

  if (table_->FindWrongTagInBuckets(i1, i2, tag)) {
    return Ok;
  } else {
    return NotFound;
  }
}

template <typename ItemType, size_t bits_per_item,
          template <size_t> class TableType, typename HashFamily>
Status CuckooFilterChangeFLength<ItemType, bits_per_item, TableType,
                                 HashFamily>::Delete(const ItemType &key) {
  size_t i1, i2;
  uint32_t tag;

  GenerateIndexTagHash(key, &i1, &tag);
  i2 = AltIndex(i1, tag);

  if (table_->DeleteTagFromBucket(i1, tag)) {
    num_items_--;
    goto TryEliminateVictim;
  } else if (table_->DeleteTagFromBucket(i2, tag)) {
    num_items_--;
    goto TryEliminateVictim;
  } else if (victim_.used && tag == victim_.tag &&
             (i1 == victim_.index || i2 == victim_.index)) {
    // num_items_--;
    victim_.used = false;
    return Ok;
  } else {
    return NotFound;
  }
TryEliminateVictim:
  if (victim_.used) {
    victim_.used = false;
    size_t i = victim_.index;
    uint32_t tag = victim_.tag;
    AddImpl(i, tag, key);
  }
  return Ok;
}

template <typename ItemType, size_t bits_per_item,
          template <size_t> class TableType, typename HashFamily>
std::string CuckooFilterChangeFLength<ItemType, bits_per_item, TableType,
                                      HashFamily>::Info() const {
  std::stringstream ss;
  ss << "CuckooFilter Status:\n"
     << "\t\t" << table_->Info() << "\n"
     << "\t\tKeys stored: " << Size() << "\n"
     << "\t\tLoad factor: " << LoadFactor() << "\n"
     << "\t\tHashtable size: " << (table_->SizeInBytes() >> 10) << " KB\n";
  if (Size() > 0) {
    ss << "\t\tbit/key:   " << BitsPerItem() << "\n";
  } else {
    ss << "\t\tbit/key:   N/A\n";
  }
  return ss.str();
}
}  // namespace cuckoofilter
#endif  // CUCKOO_FILTER_CUCKOO_FILTER_H_
