#include "cuckoofilterchange.h"
#include <assert.h>
#include <math.h>
#include <random>
#include <iostream>
#include <vector>
#include <fstream>
#include <chrono>
using cuckoofilter::CuckooFilterChangeFLength;



int main(int argc, char **argv) {
  size_t total_items = 8192*4*0.95;
  ::std::random_device random;
  size_t total_checks = 100000;

  CuckooFilterChangeFLength<size_t, 12> filter(total_items);

  // Insert
  size_t num_inserted = 0;
  for (size_t i = 0; i < total_items; i++, num_inserted++) {
    if (filter.Add(i) != cuckoofilter::Ok) {
      break;
    }
  }
  

  // Check non-existing items
  size_t total_queries = 0;
  size_t false_queries = 0;
  size_t changes = 0;
  // auto start = std::chrono::steady_clock::now();
  for (size_t i = total_items*100 ; i < total_checks + total_items*100; i++) 
  { 
    
    if (filter.Contain(i) == cuckoofilter::Ok) 
    {
      false_queries++;
      if ( filter.ChangeFingerprint(i) == cuckoofilter::Ok)
      {
       changes++;
      }
    }
    total_queries++;
  }
  
  // auto end = std::chrono::steady_clock::now();
  // std::chrono::duration<double, std::micro> elapsed = end - start;
  // std::cout << elapsed.count()<< "\n";
  // Output the measured false positive rate
  std::cout << "false_queries: "<<  false_queries  << "\n";
  std::cout << "changes: "<<  changes  << "\n";
  std::cout << "false positive rate: "<< 1.0 * false_queries / total_queries << "\n";


  return 0;
}



