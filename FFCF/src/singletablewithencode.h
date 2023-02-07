#ifndef CUCKOO_FILTER_SINGLE_TABLE_WITHENCODE_H_
#define CUCKOO_FILTER_SINGLE_TABLE_WITHENCODE_H_

#include <assert.h>
#include <sstream>
#include "bitsutil.h"
#include "debug.h"
#include "hashutil.h"
#include "printutil.h"
#include "singletabledata.h"

namespace cuckoofilter {

template <size_t bits_per_tag>
class SingleTableWithEncode {
  SingleTableData<64> *datatable_;

  static const size_t kTagsPerBucket = 4;
  static const size_t kBytesPerBucket =
      (bits_per_tag * (kTagsPerBucket + 1) + 7) >> 3;

  static const uint32_t kTagMask = (1ULL << bits_per_tag) - 1;
  static const uint32_t twokTagMask = (1ULL << (2 * bits_per_tag)) - 1;
  static const size_t kPaddingBuckets =
      ((((kBytesPerBucket + 7) / 8) * 8) - 1) / kBytesPerBucket;

  struct Bucket {
    char bits_[kBytesPerBucket];
  } __attribute__((__packed__));

  // using a pointer adds one more indirection
  Bucket *buckets_;
  size_t num_buckets_;
  TwoIndependentMultiplyShift hasher_;

  inline size_t IndexHash(uint32_t hv) const { return hv & (num_buckets_ - 1); }

  inline uint32_t TagHash(uint32_t hv) const {
    uint32_t tag;
    tag = hv & ((1ULL << (2 * bits_per_tag)) - 1);
    tag += (tag == 0);
    return tag;
  }

  inline void GenerateIndexTagHash(const uint64_t &item, uint32_t *index,
                                   uint32_t *tag) const {
    const uint64_t hash = hasher_(item);
    *index = IndexHash(hash >> 32);
    *tag = TagHash(hash);
  }

 public:
  explicit SingleTableWithEncode(const size_t num) : num_buckets_(num) {
    buckets_ = new Bucket[num_buckets_ + kPaddingBuckets];
    memset(buckets_, 0, kBytesPerBucket * (num_buckets_ + kPaddingBuckets));
    datatable_ = new SingleTableData<64>(num_buckets_);
  }

  ~SingleTableWithEncode() { delete[] buckets_; }

  size_t NumBuckets() const { return num_buckets_; }

  size_t SizeInBytes() const { return kBytesPerBucket * num_buckets_; }

  size_t SizeInTags() const { return kTagsPerBucket * num_buckets_; }

  std::string Info() const {
    std::stringstream ss;
    ss << "SingleHashtable with tag size: " << bits_per_tag << " bits \n";
    ss << "\t\tAssociativity: " << kTagsPerBucket << "\n";
    ss << "\t\tTotal # of rows: " << num_buckets_ << "\n";
    ss << "\t\tTotal # slots: " << SizeInTags() << "\n";
    return ss.str();
  }

  inline uint32_t ReadTag(const size_t i, const size_t j) const {
    const char *p = buckets_[i].bits_;
    uint32_t tag;
    const char *q = buckets_[i].bits_;

    q += (bits_per_tag >> 1);
    uint8_t a = *((uint8_t *)q);
    if (j == 4) {
      return a;
    }
    if (a == 1 || a == 2) {
      if (j == 1 || j == 3) {
        return 0;
      }

      if (bits_per_tag == 8) {
        p += j;
        tag = *((uint16_t *)p);
      } else if (bits_per_tag == 12) {
        p += (j + (j >> 1));
        tag = *((uint32_t *)p);
      } else if (bits_per_tag == 16) {
        p += (j << 1);
        tag = *((uint32_t *)p);
      }
      return tag & twokTagMask;
    }
    if (a == 3) {
      if (j == 3) {
        return 0;
      }
      if (j == 2) {
        if (bits_per_tag == 8) {
          p += j;
          tag = *((uint16_t *)p);
        } else if (bits_per_tag == 12) {
          p += (j + (j >> 1));
          tag = *((uint32_t *)p);
        } else if (bits_per_tag == 16) {
          p += (j << 1);
          tag = *((uint32_t *)p);
        }
        return tag & twokTagMask;
      } else {
        if (bits_per_tag == 8) {
          p += j;
          tag = *((uint8_t *)p);
        } else if (bits_per_tag == 12) {
          p += (j + (j >> 1));
          tag = *((uint16_t *)p) >> ((j & 1) << 2);
        } else if (bits_per_tag == 16) {
          p += (j << 1);
          tag = *((uint16_t *)p);
        }
        return tag & kTagMask;
      }
    }
    if (a == 4) {
      if (bits_per_tag == 8) {
        p += j;
        tag = *((uint8_t *)p);
      } else if (bits_per_tag == 12) {
        p += (j + (j >> 1));
        tag = *((uint16_t *)p) >> ((j & 1) << 2);
      } else if (bits_per_tag == 16) {
        p += (j << 1);
        tag = *((uint16_t *)p);
      }
      return tag & kTagMask;
    }
    return 0;
  }

  inline void WriteTag(const size_t i, const size_t j, const uint32_t t) {
    char *p = buckets_[i].bits_;
    uint32_t tag = t & twokTagMask;
    uint32_t tagshort = tag & kTagMask;
    uint32_t tagshorthigh = (tag >> bits_per_tag) & kTagMask;

    const char *q = buckets_[i].bits_;
    uint8_t a;
    q += (bits_per_tag >> 1);
    a = *((uint8_t *)q);
    if (t != 0) {
      tagshort += (tagshort == 0);
      tagshorthigh += (tagshorthigh == 0);
      if (a == 0) {
        if (j == 1 || j == 3) {
          return;
        }
        if (bits_per_tag == 8) {
          ((uint16_t *)p)[j] = tag;
        } else if (bits_per_tag == 12) {
          ((uint32_t *)p)[j] = tag;
        } else if (bits_per_tag == 16) {
          ((uint32_t *)p)[j] = tag;
        }
        *((uint8_t *)q) = 1;
        return;
      }
      if (a == 1) {
        if (j != 2) {
          return;
        }

        if (bits_per_tag == 8) {
          p += 2;
          *((uint16_t *)p) = tag;
        } else if (bits_per_tag == 12) {
          p += 3;
          *((uint32_t *)p) = tag;
        } else if (bits_per_tag == 16) {
          p += 4;
          *((uint32_t *)p) = tag;
        }
        *((uint8_t *)q) = 2;
        return;
      }
      if (a == 2) {
        if (j != 1) {
          return;
        }

        if (bits_per_tag == 8) {
          ((uint8_t *)p)[j] = tagshorthigh;
          if (*((uint8_t *)p) == 0) {
            *((uint8_t *)p) = 1;
          }
        } else if (bits_per_tag == 12) {
          if ((*((uint16_t *)p) & kTagMask) == 0) {
            *((uint8_t *)p) = 1;
          }
          p += 1;
          ((uint16_t *)p)[0] &= 0x000f;
          ((uint16_t *)p)[0] |= (tagshorthigh << 4);

        } else if (bits_per_tag == 16) {
          ((uint16_t *)p)[j] = tagshorthigh;
          if (*((uint16_t *)p) == 0) {
            *((uint16_t *)p) = 1;
          }
        }
        *((uint8_t *)q) = 3;
        return;
      }
      if (a == 3) {
        if (j == 3) {
          if (bits_per_tag == 8) {
            ((uint8_t *)p)[j] = tagshorthigh;
            p += 2;
            if (*((uint8_t *)p) == 0) {
              *((uint8_t *)p) = 1;
            }
          } else if (bits_per_tag == 12) {
            p += 3;
            if ((*((uint16_t *)p) & kTagMask) == 0) {
              *((uint8_t *)p) = 1;
            }
            p += 1;
            ((uint16_t *)p)[0] &= 0x000f;
            ((uint16_t *)p)[0] |= (tagshorthigh << 4);
          } else if (bits_per_tag == 16) {
            ((uint16_t *)p)[j] = tagshorthigh;
            p += 4;
            if (*((uint16_t *)p) == 0) {
              *((uint16_t *)p) = 1;
            }
          }
          *((uint8_t *)q) = 4;
          return;
        }
        if (j == 0) {
          if (bits_per_tag == 8) {
            ((uint8_t *)p)[j] = tagshort;
          } else if (bits_per_tag == 12) {
            p += (j + (j >> 1));
            if ((j & 1) == 0) {
              ((uint16_t *)p)[0] &= 0xf000;
              ((uint16_t *)p)[0] |= tagshort;
            } else {
              ((uint16_t *)p)[0] &= 0x000f;
              ((uint16_t *)p)[0] |= (tagshort << 4);
            }
          } else if (bits_per_tag == 16) {
            ((uint16_t *)p)[j] = tagshort;
          }
          return;
        }
        if (j == 1) {
          if (bits_per_tag == 8) {
            ((uint8_t *)p)[j] = tagshorthigh;
          } else if (bits_per_tag == 12) {
            p += (j + (j >> 1));
            if ((j & 1) == 0) {
              ((uint16_t *)p)[0] &= 0xf000;
              ((uint16_t *)p)[0] |= tagshorthigh;
            } else {
              ((uint16_t *)p)[0] &= 0x000f;
              ((uint16_t *)p)[0] |= (tagshorthigh << 4);
            }
          } else if (bits_per_tag == 16) {
            ((uint16_t *)p)[j] = tagshorthigh;
          }
          return;
        }
        return;
      }
      if (a == 4) {
        if (j == 0 || j == 2) {
          if (bits_per_tag == 8) {
            ((uint8_t *)p)[j] = tagshort;
          } else if (bits_per_tag == 12) {
            p += (j + (j >> 1));
            if ((j & 1) == 0) {
              ((uint16_t *)p)[0] &= 0xf000;
              ((uint16_t *)p)[0] |= tagshort;
            } else {
              ((uint16_t *)p)[0] &= 0x000f;
              ((uint16_t *)p)[0] |= (tagshort << 4);
            }
          } else if (bits_per_tag == 16) {
            ((uint16_t *)p)[j] = tagshort;
          }
          return;
        }
        if (j == 1 || j == 3) {
          if (bits_per_tag == 8) {
            ((uint8_t *)p)[j] = tagshorthigh;
          } else if (bits_per_tag == 12) {
            p += (j + (j >> 1));
            if ((j & 1) == 0) {
              ((uint16_t *)p)[0] &= 0xf000;
              ((uint16_t *)p)[0] |= tagshorthigh;
            } else {
              ((uint16_t *)p)[0] &= 0x000f;
              ((uint16_t *)p)[0] |= (tagshorthigh << 4);
            }
          } else if (bits_per_tag == 16) {
            ((uint16_t *)p)[j] = tagshorthigh;
          }
          return;
        }
        return;
      }
    }

    if (t == 0) {
      if (a == 0) {
        return;
      }
      if (a == 1) {
        if (j != 0) {
          return;
        }
        if (bits_per_tag == 8) {
          *((uint16_t *)p) = tag;
        } else if (bits_per_tag == 12) {
          *((uint32_t *)p) = tag;
        } else if (bits_per_tag == 16) {
          *((uint32_t *)p) = tag;
        }
        *((uint8_t *)q) = 0;
        return;
      }
      if (a == 2) {
        if (j == 1 || j == 3) {
          return;
        }
        if (j == 2) {
          if (bits_per_tag == 8) {
            p += 2;
            *((uint16_t *)p) = 0;
          } else if (bits_per_tag == 12) {
            p += 3;
            *((uint16_t *)p) = 0;
            p += 2;
            *((uint8_t *)p) = 0;
          } else if (bits_per_tag == 16) {
            p += 4;
            *((uint32_t *)p) = 0;
          }
          *((uint8_t *)q) = 1;
          return;
        }
        if (j == 0) {
          if (bits_per_tag == 8) {
            p += 2;
            tag = *((uint16_t *)p);
            tag = tag & twokTagMask;

            *((uint16_t *)p) = 0;
            p -= 2;
            *((uint16_t *)p) = tag;
          } else if (bits_per_tag == 12) {
            p += 3;
            tag = *((uint32_t *)p);
            tag = tag & twokTagMask;
            p -= 3;
            *((uint32_t *)p) = tag;

            p += 3;
            *((uint16_t *)p) = 0;
            p += 2;
            *((uint8_t *)p) = 0;
          } else if (bits_per_tag == 16) {
            p += 4;
            tag = *((uint32_t *)p);
            tag = tag & twokTagMask;

            *((uint32_t *)p) = 0;
            p -= 4;
            *((uint32_t *)p) = tag;
          }
          *((uint8_t *)q) = 1;
          return;
        }
      }
      if (a == 3) {
        if (j == 0 || j == 1) {
          if (bits_per_tag == 8) {
            p += 2;
            tag = *((uint16_t *)p);
            tag = tag & twokTagMask;
            *((uint16_t *)p) = 0;
            p -= 2;
            *((uint16_t *)p) = tag;

          } else if (bits_per_tag == 12) {
            p += 3;
            tag = *((uint32_t *)p);
            tag = tag & twokTagMask;
            *((uint32_t *)p) = 0;
            p -= 3;
            *((uint32_t *)p) = tag;
          } else if (bits_per_tag == 16) {
            p += 4;
            tag = *((uint32_t *)p);
            tag = tag & twokTagMask;
            *((uint32_t *)p) = 0;
            p -= 4;
            *((uint32_t *)p) = tag;
          }
          *((uint8_t *)q) = 1;
          return;
        }
        if (j == 2) {
          if (bits_per_tag == 8) {
            *((uint32_t *)p) = 0;
          } else if (bits_per_tag == 12) {
            *((uint32_t *)p) = 0;
            p += 3;
            *((uint32_t *)p) = 0;
          } else if (bits_per_tag == 16) {
            *((uint64_t *)p) = 0;
          }
          *((uint8_t *)q) = 0;
          return;
        }
        return;
      }
      if (a == 4) {
        if (bits_per_tag == 8) {
          *((uint32_t *)p) = 0;
        } else if (bits_per_tag == 12) {
          *((uint32_t *)p) = 0;
          p += 3;
          *((uint32_t *)p) = 0;
        } else if (bits_per_tag == 16) {
          *((uint64_t *)p) = 0;
        }
        *((uint8_t *)q) = 0;
        return;
      }
    }
    return;
  }

  inline bool FindTagInBuckets(const size_t i1, const size_t i2,
                               const uint32_t tag) const {
    uint32_t tagshort = tag & kTagMask;
    tagshort += (tagshort == 0);
    uint32_t tagshorthigh = (tag >> bits_per_tag) & kTagMask;
    tagshorthigh += (tagshorthigh == 0);
    int a1 = ReadTag(i1, 4);
    int a2 = ReadTag(i2, 4);

    if (a1 == 1) {
      if ((ReadTag(i1, 0) == tag)) {
        return true;
      }
    }
    if (a1 == 2) {
      if ((ReadTag(i1, 0) == tag) || (ReadTag(i1, 2) == tag)) {
        return true;
      }
    }
    if (a1 == 3) {
      if ((ReadTag(i1, 0) == tagshort) || (ReadTag(i1, 1) == tagshorthigh) ||
          (ReadTag(i1, 2) == tag)) {
        return true;
      }
    }
    if (a1 == 4) {
      if ((ReadTag(i1, 0) == tagshort) || (ReadTag(i1, 1) == tagshorthigh) ||
          (ReadTag(i1, 2) == tagshort) || (ReadTag(i1, 3) == tagshorthigh)) {
        return true;
      }
    }

    if (a2 == 1) {
      if ((ReadTag(i2, 0) == tag)) {
        return true;
      }
    }
    if (a2 == 2) {
      if ((ReadTag(i2, 0) == tag) || (ReadTag(i2, 2) == tag)) {
        return true;
      }
    }
    if (a2 == 3) {
      if ((ReadTag(i2, 0) == tagshort) || (ReadTag(i2, 1) == tagshorthigh) ||
          (ReadTag(i2, 2) == tag)) {
        return true;
      }
    }
    if (a2 == 4) {
      if ((ReadTag(i2, 0) == tagshort) || (ReadTag(i2, 1) == tagshorthigh) ||
          (ReadTag(i2, 2) == tagshort) || (ReadTag(i2, 3) == tagshorthigh)) {
        return true;
      }
    }

    return false;
  }

  inline bool FindTagInBucket(const size_t i, const uint32_t tag) const {
    uint32_t tagshort = tag & kTagMask;
    tagshort += (tagshort == 0);
    uint32_t tagshorthigh = (tag >> bits_per_tag) & kTagMask;
    tagshorthigh += (tagshorthigh == 0);
    uint32_t tagread1;
    for (size_t j = 0; j < kTagsPerBucket; j++) {
      tagread1 = ReadTag(i, j);

      if (j == 0 || j == 2) {
        if ((tagread1 == tagshort) || (tagread1 == tag)) {
          return true;
        }
      }
      if (j == 1 || j == 3) {
        if (tagread1 == tagshorthigh) {
          return true;
        }
      }
    }
    return false;
  }
  inline bool FindWrongTagInBuckets(const size_t i1, const size_t i2,
                                    const uint32_t tag) {
    uint64_t olditem, olditem2;
    uint32_t taghasmeans;
    uint32_t indexnomeans;

    uint32_t tagshort = tag & kTagMask;
    tagshort += (tagshort == 0);
    uint32_t tagshorthigh = (tag >> bits_per_tag) & kTagMask;
    tagshorthigh += (tagshorthigh == 0);
    uint32_t tagread1, tagread2;
    tagread1 = ReadTag(i1, 4);

    if (tagread1 == 3) {
      if ((ReadTag(i1, 0) == tagshort) || (ReadTag(i1, 1) == tagshorthigh)) {
        olditem = datatable_->ReadTag(i1, 0);
        olditem2 = olditem;
        GenerateIndexTagHash(olditem, &indexnomeans, &taghasmeans);
        WriteTag(i1, 1, taghasmeans);
        olditem = datatable_->ReadTag(i1, 1);
        GenerateIndexTagHash(olditem, &indexnomeans, &taghasmeans);
        WriteTag(i1, 0, taghasmeans);
        datatable_->WriteTag(i1, 0, olditem);
        datatable_->WriteTag(i1, 1, olditem2);
        return true;
      }
    }
    if (tagread1 == 4) {
      if ((ReadTag(i1, 0) == tagshort) || (ReadTag(i1, 1) == tagshorthigh)) {
        olditem = datatable_->ReadTag(i1, 0);
        olditem2 = olditem;
        GenerateIndexTagHash(olditem, &indexnomeans, &taghasmeans);
        WriteTag(i1, 1, taghasmeans);
        olditem = datatable_->ReadTag(i1, 1);
        GenerateIndexTagHash(olditem, &indexnomeans, &taghasmeans);
        WriteTag(i1, 0, taghasmeans);
        datatable_->WriteTag(i1, 0, olditem);
        datatable_->WriteTag(i1, 1, olditem2);
        return true;
      }
      if ((ReadTag(i1, 2) == tagshort) || (ReadTag(i1, 3) == tagshorthigh)) {
        olditem = datatable_->ReadTag(i1, 2);
        olditem2 = olditem;
        GenerateIndexTagHash(olditem, &indexnomeans, &taghasmeans);
        WriteTag(i1, 3, taghasmeans);
        olditem = datatable_->ReadTag(i1, 3);
        GenerateIndexTagHash(olditem, &indexnomeans, &taghasmeans);
        WriteTag(i1, 2, taghasmeans);
        datatable_->WriteTag(i1, 2, olditem);
        datatable_->WriteTag(i1, 3, olditem2);
        return true;
      }
    }

    tagread2 = ReadTag(i2, 4);
    if (tagread2 == 3) {
      if ((ReadTag(i2, 0) == tagshort) || (ReadTag(i2, 1) == tagshorthigh)) {
        olditem = datatable_->ReadTag(i2, 0);
        olditem2 = olditem;
        GenerateIndexTagHash(olditem, &indexnomeans, &taghasmeans);
        WriteTag(i2, 1, taghasmeans);
        olditem = datatable_->ReadTag(i2, 1);
        GenerateIndexTagHash(olditem, &indexnomeans, &taghasmeans);
        WriteTag(i2, 0, taghasmeans);
        datatable_->WriteTag(i2, 0, olditem);
        datatable_->WriteTag(i2, 1, olditem2);
        return true;
      }
    }
    if (tagread2 == 4) {
      if ((ReadTag(i2, 0) == tagshort) || (ReadTag(i2, 1) == tagshorthigh)) {
        olditem = datatable_->ReadTag(i2, 0);
        olditem2 = olditem;
        GenerateIndexTagHash(olditem, &indexnomeans, &taghasmeans);
        WriteTag(i2, 1, taghasmeans);
        olditem = datatable_->ReadTag(i2, 1);
        GenerateIndexTagHash(olditem, &indexnomeans, &taghasmeans);
        WriteTag(i2, 0, taghasmeans);
        datatable_->WriteTag(i2, 0, olditem);
        datatable_->WriteTag(i2, 1, olditem2);
        return true;
      }
      if ((ReadTag(i2, 2) == tagshort) || (ReadTag(i2, 3) == tagshorthigh)) {
        olditem = datatable_->ReadTag(i2, 2);
        olditem2 = olditem;
        GenerateIndexTagHash(olditem, &indexnomeans, &taghasmeans);
        WriteTag(i2, 3, taghasmeans);
        olditem = datatable_->ReadTag(i2, 3);
        GenerateIndexTagHash(olditem, &indexnomeans, &taghasmeans);
        WriteTag(i2, 2, taghasmeans);
        datatable_->WriteTag(i2, 2, olditem);
        datatable_->WriteTag(i2, 3, olditem2);
        return true;
      }
    }
    return false;
  }

  inline bool DeleteTagFromBucket(const size_t i, const uint32_t tag) {
    uint32_t tagshort = tag & kTagMask;
    tagshort += (tagshort == 0);
    uint32_t tagshorthigh = (tag >> bits_per_tag) & kTagMask;
    tagshorthigh += (tagshorthigh == 0);

    uint32_t tagread1;
    uint32_t a = ReadTag(i, 4);
    uint64_t olditem;
    uint32_t taghasmeans;
    uint32_t indexnomeans;
    uint32_t j = 10;
    if (a == 0) {
      return false;
    }
    if (a == 1) {
      tagread1 = ReadTag(i, 0);
      if (tagread1 == tag) {
        assert(FindTagInBucket(i, tag) == true);
        WriteTag(i, 0, 0);
        datatable_->WriteTag(i, 0, 0);
        return true;
      }
    }
    if (a == 2) {
      tagread1 = ReadTag(i, 2);
      if (tagread1 == tag) {
        assert(FindTagInBucket(i, tag) == true);
        WriteTag(i, 2, 0);
        datatable_->WriteTag(i, 2, 0);
        return true;
      }

      tagread1 = ReadTag(i, 0);
      if (tagread1 == tag) {
        assert(FindTagInBucket(i, tag) == true);
        WriteTag(i, 0, 0);

        olditem = datatable_->ReadTag(i, 2);
        datatable_->WriteTag(i, 0, olditem);
        datatable_->WriteTag(i, 2, 0);
        return true;
      }
    }
    if (a == 3) {
      if (ReadTag(i, 0) == tagshort) {
        j = 0;
      }
      if (ReadTag(i, 1) == tagshorthigh) {
        j = 1;
      }
      if (j == 1 && (ReadTag(i, 0) == tagshort)) {
        olditem = datatable_->ReadTag(i, 0);
        GenerateIndexTagHash(olditem, &indexnomeans, &taghasmeans);
        if (tag == taghasmeans) {
          j = 0;
        }
        olditem = datatable_->ReadTag(i, 1);
        GenerateIndexTagHash(olditem, &indexnomeans, &taghasmeans);
        if (tag == taghasmeans) {
          j = 1;
        }
      }

      if (j == 0) {
        WriteTag(i, 0, 0);
        olditem = datatable_->ReadTag(i, 2);
        datatable_->WriteTag(i, 0, olditem);
        olditem = datatable_->ReadTag(i, 1);
        datatable_->WriteTag(i, 2, olditem);
        datatable_->WriteTag(i, 1, 0);
        GenerateIndexTagHash(olditem, &indexnomeans, &taghasmeans);

        WriteTag(i, 2, taghasmeans);
        return true;
      }

      if (j == 1) {
        WriteTag(i, 1, 0);
        olditem = datatable_->ReadTag(i, 0);
        datatable_->WriteTag(i, 1, olditem);
        olditem = datatable_->ReadTag(i, 2);
        datatable_->WriteTag(i, 0, olditem);
        olditem = datatable_->ReadTag(i, 1);
        datatable_->WriteTag(i, 2, olditem);
        GenerateIndexTagHash(olditem, &indexnomeans, &taghasmeans);

        WriteTag(i, 2, taghasmeans);
        datatable_->WriteTag(i, 1, 0);
        return true;
      }
      tagread1 = ReadTag(i, 2);
      if (tagread1 == tag) {
        assert(FindTagInBucket(i, tag) == true);
        WriteTag(i, 2, 0);
        olditem = datatable_->ReadTag(i, 0);
        GenerateIndexTagHash(olditem, &indexnomeans, &taghasmeans);
        WriteTag(i, 0, taghasmeans);

        olditem = datatable_->ReadTag(i, 1);
        datatable_->WriteTag(i, 1, 0);
        datatable_->WriteTag(i, 2, olditem);
        GenerateIndexTagHash(olditem, &indexnomeans, &taghasmeans);
        WriteTag(i, 2, taghasmeans);

        return true;
      }
    }
    if (a == 4) {
      uint32_t j0 = 0;
      uint32_t j1 = 0;
      uint32_t j2 = 0;
      uint32_t j3 = 0;
      if (ReadTag(i, 0) == tagshort) {
        j = 0;
        j0 = 1;
      }
      if (ReadTag(i, 1) == tagshorthigh) {
        j = 1;
        j1 = 1;
      }
      if (ReadTag(i, 2) == tagshort) {
        j = 2;
        j2 = 1;
      }
      if (ReadTag(i, 3) == tagshorthigh) {
        j = 3;
        j3 = 1;
      }
      if (j0 + j1 + j2 + j3 > 1) {
        olditem = datatable_->ReadTag(i, 0);
        GenerateIndexTagHash(olditem, &indexnomeans, &taghasmeans);
        if (tag == taghasmeans) {
          j = 0;
        }
        olditem = datatable_->ReadTag(i, 1);
        GenerateIndexTagHash(olditem, &indexnomeans, &taghasmeans);
        if (tag == taghasmeans) {
          j = 1;
        }
        olditem = datatable_->ReadTag(i, 2);
        GenerateIndexTagHash(olditem, &indexnomeans, &taghasmeans);
        if (tag == taghasmeans) {
          j = 2;
        }
        olditem = datatable_->ReadTag(i, 3);
        GenerateIndexTagHash(olditem, &indexnomeans, &taghasmeans);
        if (tag == taghasmeans) {
          j = 3;
        }
      }

      if (j == 0) {
        WriteTag(i, 0, 0);
        olditem = datatable_->ReadTag(i, 3);
        datatable_->WriteTag(i, 0, olditem);
        GenerateIndexTagHash(olditem, &indexnomeans, &taghasmeans);
        WriteTag(i, 0, taghasmeans);

        datatable_->WriteTag(i, 3, 0);
        olditem = datatable_->ReadTag(i, 2);
        GenerateIndexTagHash(olditem, &indexnomeans, &taghasmeans);
        WriteTag(i, 2, taghasmeans);

        olditem = datatable_->ReadTag(i, 1);
        GenerateIndexTagHash(olditem, &indexnomeans, &taghasmeans);
        WriteTag(i, 1, taghasmeans);

        return true;
      }

      if (j == 1) {
        WriteTag(i, 0, 0);

        olditem = datatable_->ReadTag(i, 0);
        GenerateIndexTagHash(olditem, &indexnomeans, &taghasmeans);
        WriteTag(i, 0, taghasmeans);

        olditem = datatable_->ReadTag(i, 2);
        GenerateIndexTagHash(olditem, &indexnomeans, &taghasmeans);
        WriteTag(i, 2, taghasmeans);

        olditem = datatable_->ReadTag(i, 3);
        GenerateIndexTagHash(olditem, &indexnomeans, &taghasmeans);
        WriteTag(i, 1, taghasmeans);
        datatable_->WriteTag(i, 3, 0);
        datatable_->WriteTag(i, 1, olditem);
        return true;
      }

      if (j == 2) {
        WriteTag(i, 0, 0);

        olditem = datatable_->ReadTag(i, 0);
        GenerateIndexTagHash(olditem, &indexnomeans, &taghasmeans);
        WriteTag(i, 0, taghasmeans);

        olditem = datatable_->ReadTag(i, 3);
        GenerateIndexTagHash(olditem, &indexnomeans, &taghasmeans);
        WriteTag(i, 2, taghasmeans);
        datatable_->WriteTag(i, 3, 0);
        datatable_->WriteTag(i, 2, olditem);

        olditem = datatable_->ReadTag(i, 1);
        GenerateIndexTagHash(olditem, &indexnomeans, &taghasmeans);
        WriteTag(i, 1, taghasmeans);

        return true;
      }

      if (j == 3) {
        WriteTag(i, 0, 0);

        olditem = datatable_->ReadTag(i, 0);
        GenerateIndexTagHash(olditem, &indexnomeans, &taghasmeans);
        WriteTag(i, 0, taghasmeans);

        olditem = datatable_->ReadTag(i, 2);
        GenerateIndexTagHash(olditem, &indexnomeans, &taghasmeans);
        WriteTag(i, 2, taghasmeans);
        datatable_->WriteTag(i, 3, 0);

        olditem = datatable_->ReadTag(i, 1);
        GenerateIndexTagHash(olditem, &indexnomeans, &taghasmeans);
        WriteTag(i, 1, taghasmeans);

        return true;
      }
    }

    return false;
  }

  inline bool InsertTagToBucket(const size_t i, const uint32_t tag,
                                const bool kickout, uint32_t &oldtag,
                                const uint64_t item, uint64_t &olditem) {
    uint32_t a = ReadTag(i, 4);
    if (a == 0) {
      WriteTag(i, 0, tag);
      datatable_->WriteTag(i, 0, item);
      return true;
    }
    if (a == 1) {
      WriteTag(i, 2, tag);
      datatable_->WriteTag(i, 2, item);
      return true;
    }
    if (a == 2) {
      WriteTag(i, 1, tag);
      datatable_->WriteTag(i, 1, item);
      return true;
    }
    if (a == 3) {
      WriteTag(i, 3, tag);
      datatable_->WriteTag(i, 3, item);
      return true;
    }

    if (a == 4 && kickout) {
      size_t r = rand() % kTagsPerBucket;
      olditem = datatable_->ReadTag(i, r);
      WriteTag(i, r, tag);
      datatable_->WriteTag(i, r, item);
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

  inline size_t BucketInfo(const size_t i) const {
    size_t num = 0;
    for (size_t j = 0; j < 8192; j++) {
      std::cout << ReadTag(j, 4) << std::endl;
    }
    return 0;
  }
};
}  // namespace cuckoofilter
#endif  // CUCKOO_FILTER_SINGLE_TABLE_H_
