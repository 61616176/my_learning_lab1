#include "execution/executors/window_function_executor.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/*
 *start first pos of duplicate value
 *end first pos of new value
 *idx current idx to find duplicate value
 *return false means that there is no more duplicate value in the rest of vector
 */
auto WindowFindDuplicateRange(const std::vector<WindowFunctionKey>::iterator &search_begin,
                              const std::vector<WindowFunctionKey>::iterator &search_end, std::vector<WindowFunctionKey>::iterator &start,
                              std::vector<WindowFunctionKey>::iterator &last, uint32_t idx) -> bool {
  const auto &it = std::adjacent_find(search_begin, search_end, [idx](const WindowFunctionKey &first, const WindowFunctionKey &second) {
    if (first.second.at(idx).CompareEquals(second.second.at(idx)) == CmpBool::CmpTrue) {
      return true;
    }
    return false;
  });

  // 如果找到了重复的相邻元素，我们需要找到这个重复值区间的结束位置
  if (it != search_end) {
    // 找到重复值的起始位置
    start = it;
    // 跳过重复值，指向下一个可能不重复的值
    last = it + 2;
    while (last != search_end) {
      if (last->second.at(idx).CompareEquals(it->second.at(idx)) == CmpBool::CmpTrue) {
        last++;
      } else {
        break;
      }
    }
    return true;
  }
  return false;
}

void WindowFunctionExecutor::WindowFunctionAggregateAndSort() {
  Tuple child_tuple{};
  RID child_rid{};
  std::unordered_map<uint32_t, AggregateKey> key_for_empty_insert;
  std::vector<uint32_t> window_indeces;
  const Schema &schema = child_executor_->GetOutputSchema();
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    std::vector<Value> windowfunctionkey_first;
    std::vector<Value> windowfunctionkey_second;
    std::vector<std::pair<AggregateKey, AggregateValue>> windowfunctionkey_third;
    uint32_t col_idx{0};
    for (const auto &col : plan_->columns_) {
      // aggregate
      if (col->GetChildren().size() == 0) {
        //在这一步insertcombine？
        // windowfunctionkey_second.push_back(GenerateInitialWindowFunctionValue(plan_->window_functions_[idx++].type_))
        auto &window_function = plan_->window_functions_.at(col_idx);
        //生成key、value
        AggregateKey key;
        AggregateValue value;
        for (const auto &partition_by : window_function.partition_by_) {
          key.group_bys_.push_back(partition_by->Evaluate(&child_tuple, schema));
        }
        key_for_empty_insert[col_idx] = key;

        const auto &function = window_function.function_;
        value.aggregates_.push_back(function->Evaluate(&child_tuple, schema));

        windowfunctionkey_third.push_back(std::make_pair<AggregateKey, AggregateValue>(std::move(key), std::move(value)));
        window_indeces.push_back(col_idx);
        //插入对应的aht_map_中
        //读取值插入table中, 确定只返回一个Value

        //assert(aht_map_[col_idx] != nullptr);
        //auto res = aht_map_[col_idx]->InsertCombine(key, value);
        
        windowfunctionkey_second.push_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
      } else {
        windowfunctionkey_second.push_back(col->Evaluate(&child_tuple, schema));
      }
      col_idx++;
    }
    for (const auto &order_by : plan_->window_functions_.at(0).order_by_) {
      windowfunctionkey_first.push_back(order_by.second->Evaluate(&child_tuple, schema));
    }
    sorted_table_.push_back({windowfunctionkey_first, windowfunctionkey_second, windowfunctionkey_third});
  }

  if (plan_->window_functions_.at(0).order_by_.empty()) {
    
    return;
  }
  // begin sort
  int idx{0};

  assert(plan_->window_functions_.at(0).order_by_.size() != 0);
  auto order_by_type = plan_->window_functions_.at(0).order_by_.at(idx).first;


  auto Sort = [&](const WindowFunctionKey &first, const WindowFunctionKey &second) {
    if (order_by_type == OrderByType::DEFAULT or order_by_type == OrderByType::ASC) {
      if (first.second.at(idx).CompareLessThan(second.second.at(idx)) == CmpBool::CmpTrue) {
        return true;
      }
    } else {
      if (first.second.at(idx).CompareGreaterThan(second.second.at(idx)) == CmpBool::CmpTrue) {
        return true;
      }
    }
    return false;
  };

  std::sort(sorted_table_.begin(), sorted_table_.end(), Sort);
  // std::cout << "sorted_table_ size: " << sorted_table_.size() << std::endl;
  for (const auto &i : plan_->window_functions_.at(0).order_by_) {
    if (i == plan_->window_functions_.at(0).order_by_[0]) {
      continue;
    }
    idx++;
    auto begin = sorted_table_.begin();
    auto end = sorted_table_.end();
    auto start = begin;
    auto last = end;
    while (/*遍历完成*/ WindowFindDuplicateRange(begin, end, start, last, idx - 1)) {
      order_by_type = plan_->window_functions_.at(0).order_by_[idx].first;
      std::sort(start, last, Sort);
      begin = last;
    }
  }

  //aggregate
  //遍历sort_table, 针对每个third进行insertCombine
  //取值插入sort_table.first对应的index里
  for (auto &row : sorted_table_) {
    auto &window_function_keys = row.third;
    uint32_t idx{0};
    for (auto &window_function_key : window_function_keys) {
      uint32_t window_index = window_indeces.at(idx++);
      auto res = aht_map_[window_index]->InsertCombine(window_function_key.first, window_function_key.second);
      row.first.at(window_index) = res[0];
    }
  }
  
  if (plan_->window_functions_.at(0).order_by_.empty()) {
    for (auto &row : sorted_table_) {
      for (auto &key : key_for_empty_insert) {
        row.first.at(key.first) = aht_map_[key.first]->GetMap().at(key.second).aggregates_.at(0);
      }
    }
  }
}

WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void WindowFunctionExecutor::Init() {
  child_executor_->Init();

  // init aggregate table
  for (const auto &window_function : plan_->window_functions_) {
    //std::cout << "window index: " << window_function.first << "type: " << static_cast<int>(window_function.second.type_) << std::endl;
    std::vector<AbstractExpressionRef> function= {window_function.second.function_};
    std::vector<WindowFunctionType> type = {window_function.second.type_};
    auto aht = std::make_unique<WindowFunctionHashTable>(function, type);
    aht_map_[window_function.first] = std::move(aht);
  }

  // aggregate and sort
  WindowFunctionAggregateAndSort();
}

auto WindowFunctionExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!status_) {
    return false;
  }


  auto sort_key = sorted_table_.front();
  auto vals = sort_key.first;
  *tuple = Tuple(vals, &GetOutputSchema());

  sorted_table_.erase(sorted_table_.begin());
  if (sorted_table_.empty()) {
    status_ = false;
  }
  return true;

  // reuse aggregate的next
}
}  // namespace bustub
