Flexible Fingerprint Cuckoo Filter
============

A cuckoo filter with flexible fingerprints supports following operations:

*  `Add(item): insert an item to the filter
*  `Contain(item): return if item is already in the filter. 
*  `Delete(item): delete the given item from the filter. 
*  `ChangeFingerprint(item): Enable error correction for an item when it causes a false positive. 

`example/test.cc` is a simple example

To build the example (`example/test.cc`):
```bash
$ make test
```




