#include "concurrency/watermark.h"
#include <exception>
#include "common/exception.h"

namespace bustub {
void print(std::string_view text, const std::deque<timestamp_t>& v = {}, int num=10)
{
    int count{0};
    std::cout << text << ": ";
    for (const auto& e : v) {
      std::cout << e << ' ';
      count++;
      if (count == num-1) 
        break;
    }
        
    std::cout << '\n';
}
auto Watermark::AddTxn(timestamp_t read_ts) -> void {
  if (read_ts < commit_ts_) {
    throw Exception("read ts < commit ts");
  }

  // TODO(fall2023): implement me!
  if (current_reads_.find(read_ts) != current_reads_.end()) {
    current_reads_[read_ts]++;
  } else {
    current_reads_.insert({read_ts, 1});
    //min_heap_.push_back(read_ts);
    //std::push_heap(min_heap_.begin(), min_heap_.end(), std::greater<timestamp_t>()); // 这一步还有待商榷
  
    int index = sorted_.size();
    sorted_.push_back(read_ts);
    sorted_indices.insert({read_ts, index});
    if (index == 0) {
      watermark_ = read_ts;
    }
  }
}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
  // TODO(fall2023): implement me!
  if (current_reads_[read_ts] == 1) {
    current_reads_.erase(read_ts);

    if (read_ts == watermark_) {
      sorted_.pop_front();
      watermark_ = sorted_.front();
    } else{
      //auto it = std::find(sorted_.begin(), sorted_.end(), read_ts);
      auto it = sorted_.begin() + sorted_indices[read_ts];
      sorted_.erase(it);
    }
  } else {
    current_reads_[read_ts]--;
  }
}

}  // namespace bustub
