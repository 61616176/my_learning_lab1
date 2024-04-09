//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// topn_executor.h
//
// Identification: src/include/execution/executors/topn_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/executors/sort_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/topn_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * The TopNExecutor executor executes a topn.
 */
class TopNExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new TopNExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The TopN plan to be executed
   */
  TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the TopN */
  void Init() override;

  /**
   * Yield the next tuple from the TopN.
   * @param[out] tuple The next tuple produced by the TopN
   * @param[out] rid The next tuple RID produced by the TopN
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the TopN */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  /** Sets new child executor (for testing only) */
  void SetChildExecutor(std::unique_ptr<AbstractExecutor> &&child_executor) {
    child_executor_ = std::move(child_executor);
  }

  /** @return The size of top_entries_ container, which will be called on each child_executor->Next(). */
  auto GetNumInHeap() -> size_t;

  /*升序排列要用大顶推，求最小的前n个元素， 堆顶为最小中的最大,比较时用lessthan，如果小于加入；构建时用greaterthan
   *降序排列用小顶堆，求最大的前n个元素，堆顶为最大中的最小，比较用greateerthan，如果大于加入；构建时用lessthan
   *和堆顶比较时用正常的比较方法 建堆时用相反的比较方法
   */
  class TopNDeque {
   public:
    TopNDeque() = default;

    TopNDeque(std::size_t capacity, std::vector<std::pair<OrderByType, AbstractExpressionRef>> order_bys)
        : capacity_(capacity) {
      for (const auto &order_by : order_bys) {
        comp_types_.push_back(order_by.first);
      }
    }

    ~TopNDeque() = default;

    void Push(const sortKey &key) {
      //std::cout << std::endl;
      //std::cout << "begin  push\n";
      if (deque_.size() < capacity_) {
        deque_.push_back(key);
      } else if (Compare_(key, deque_.front())) {
        //应该将堆顶换掉
        std::make_heap(deque_.begin(), deque_.end(), Compare_);
        deque_[0] = key;
      }

      std::make_heap(deque_.begin(), deque_.end(), Compare_);

      Print();
    }

    auto Pop(sortKey &key) -> bool {
      if (Size() == 0) {
        return false;
      }
      make_compare_ = true;
      std::make_heap(deque_.begin(), deque_.end(), Compare_);
      make_compare_ = false;
      key = deque_.front();
      deque_.erase(deque_.begin());
      return true;
    }

    void Print() const {
      //std::cout << "************************\n";
      for (const auto &i : deque_) {
        for (const auto &val : i.first) {
          //std::cout << val.ToString() << " ";
        }
        //std::cout << "|| "; 
        for (const auto &val : i.second) {
          //std::cout << val.ToString() << " ";
        }
        //std::cout << std::endl;
      }
    }

    auto Size() const -> std::size_t { return deque_.size(); }

    auto Capacity() const -> std::size_t { return capacity_; }

    auto Begin() -> std::vector<sortKey>::iterator { return deque_.begin(); }

    auto End() -> std::vector<sortKey>::iterator { return deque_.end(); }

    /*only for test*/
    void ShowCompTypes() {
      //std::cout << "show comp types\n";
      //std::cout << comp_types_.size() << std::endl;
      for (const auto &i : comp_types_) {
        //std::cout << static_cast<int>(i) << " ";
      }
      //std::cout << std::endl;
    }

   private:
    std::size_t capacity_;
    std::vector<sortKey> deque_;
    std::vector<OrderByType> comp_types_;

    /*represent it's making heap or checking heap top*/
    bool make_compare_{false};

    std::function<bool(const sortKey &, const sortKey &, uint32_t)> GreaterCompare_ =
        [&](const sortKey &first, const sortKey &second, uint32_t idx) {
          return first.second.at(idx).CompareGreaterThan(second.second.at(idx)) == CmpBool::CmpTrue;
        };
    std::function<bool(const sortKey &, const sortKey &, uint32_t)> LessCompare_ =
        [&](const sortKey &first, const sortKey &second, uint32_t idx) {
          return first.second.at(idx).CompareLessThan(second.second.at(idx)) == CmpBool::CmpTrue;
        };
    std::function<bool(const sortKey &, const sortKey &, uint32_t)> EqualCompare_ =
        [&](const sortKey &first, const sortKey &second, uint32_t idx) {
          return first.second.at(idx).CompareEquals(second.second.at(idx)) == CmpBool::CmpTrue;
        };

    std::function<bool(const sortKey &, const sortKey &)> Compare_ = [&](const sortKey &first, const sortKey &second) {
      //std::cout << "make compare: " << make_compare_ << std::endl;
      BUSTUB_ASSERT(comp_types_.empty() == false, "comp_types_ must not be empty!");
      for (uint32_t idx = 0; idx < comp_types_.size(); idx++) {
        if (EqualCompare_(first, second, idx)) {
          //std::cout << "equal\n";
          continue;
        }

        if (comp_types_[idx] == OrderByType::DEFAULT || comp_types_[idx] == OrderByType::ASC) {
          if (make_compare_) {
            //std::cout << "建立大顶堆\n";
            return GreaterCompare_(first, second, idx);
          }
          //std::cout << "升序， 比较谁小\n";
          return LessCompare_(first, second, idx);
        }

        if (make_compare_) {
          //std::cout << "建立小顶堆\n";
          return LessCompare_(first, second, idx);
        }
        //std::cout << "降序，比较谁大\n";
        return GreaterCompare_(first, second, idx);
      }
      // if go here ,wrong!
      return false;
    };
  };

 private:
  /** The TopN plan node to be executed */
  const TopNPlanNode *plan_;
  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;

  std::unique_ptr<TopNDeque> topn_deque_;

};
}  // namespace bustub
