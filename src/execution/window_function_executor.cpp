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
                              const std::vector<WindowFunctionKey>::iterator &search_end,
                              std::vector<WindowFunctionKey>::iterator &start,
                              std::vector<WindowFunctionKey>::iterator &last, uint32_t idx) -> bool {
  const auto &it = std::adjacent_find(
      search_begin, search_end, [idx](const WindowFunctionKey &first, const WindowFunctionKey &second) {
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
  std::cout << "1\n";
  bool first_flag{true};
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    std::vector<Value> windowfunctionkey_first;
    std::vector<Value> windowfunctionkey_second;
    std::vector<std::pair<AggregateKey, AggregateValue>> windowfunctionkey_third;
    uint32_t col_idx{0};
    for (const auto &col : plan_->columns_) {
      // aggregate
      std::cout <<"col type : " <<col->ToString() <<std::endl;
      if (col->ToString() == "#0.4294967295") { // 假定这个值是placeholder的标志
        //在这一步insertcombine？
        // windowfunctionkey_second.push_back(GenerateInitialWindowFunctionValue(plan_->window_functions_[idx++].type_))
        auto &window_function = plan_->window_functions_.at(col_idx);
        //生成key、value
        AggregateKey key;
        AggregateValue value;
        for (const auto &partition_by : window_function.partition_by_) {
          std::cout << "window function type: " << static_cast<int>(window_function.type_) << "window function partiton: " << partition_by->ToString() << std::endl; 
          key.group_bys_.push_back(partition_by->Evaluate(&child_tuple, schema));
          std::cout << "col index: " << col_idx  << "partition key: " << partition_by->Evaluate(&child_tuple, schema).ToString() << std::endl;
        }
        key_for_empty_insert[col_idx] = key;

        const auto &function = window_function.function_;
        value.aggregates_.push_back(function->Evaluate(&child_tuple, schema));

        windowfunctionkey_third.push_back(
            std::make_pair<AggregateKey, AggregateValue>(std::move(key), std::move(value)));
        if (first_flag) {
          window_indeces.push_back(col_idx);
        }
        
        //插入对应的aht_map_中
        //读取值插入table中, 确定只返回一个Value

        // assert(aht_map_[col_idx] != nullptr);
        // auto res = aht_map_[col_idx]->InsertCombine(key, value);

        windowfunctionkey_first.push_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
      } else {
        windowfunctionkey_first.push_back(col->Evaluate(&child_tuple, schema));
      }
      col_idx++;
    }
    for (const auto &order_by : plan_->window_functions_.at(window_indeces[0]).order_by_) {
      windowfunctionkey_second.push_back(order_by.second->Evaluate(&child_tuple, schema));
    }
    sorted_table_.push_back({windowfunctionkey_first, windowfunctionkey_second, windowfunctionkey_third});
    first_flag = false;
  }
  std::cout << "2\n";
  if (!plan_->window_functions_.at(window_indeces[0]).order_by_.empty()) {
    // begin sort
    int idx{0};

    assert(plan_->window_functions_.at(window_indeces[0]).order_by_.size() != 0);
    auto order_by_type = plan_->window_functions_.at(window_indeces[0]).order_by_.at(idx).first;

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
    for (const auto &i : plan_->window_functions_.at(window_indeces[0]).order_by_) {
      if (i == plan_->window_functions_.at(window_indeces[0]).order_by_[0]) {
        continue;
      }
      idx++;
      auto begin = sorted_table_.begin();
      auto end = sorted_table_.end();
      auto start = begin;
      auto last = end;
      while (/*遍历完成*/ WindowFindDuplicateRange(begin, end, start, last, idx - 1)) {
        order_by_type = plan_->window_functions_.at(window_indeces[0]).order_by_[idx].first;
        std::sort(start, last, Sort);
        begin = last;
      }
    }
  }
  std::cout << "3\n";
  // aggregate
  //遍历sort_table, 针对每个third进行insertCombine
  //取值插入sort_table.first对应的index里
  std::cout << "row size: " << sorted_table_.front().first.size() << std::endl;

  for (auto row=sorted_table_.begin(); row != sorted_table_.end(); row++) {
    auto &window_function_keys = row->third;
    uint32_t idx{0};
    for (auto &window_function_key : window_function_keys) {
      uint32_t window_index = window_indeces.at(idx++);
      std::cout << "window index:" << window_index << std::endl;
      if (plan_->window_functions_.at(window_index).type_ == WindowFunctionType::Rank) {
        std::cout << "rank\n";
        //deal with previous row
        AggregateValue rank_value;
        auto IsDuplicate = [](std::vector<Value> first, std::vector<Value> second) -> bool{
          for (uint32_t idx=0; idx != first.size(); idx++) {
            if (first.at(idx).CompareEquals(second.at(idx)) == CmpBool::CmpTrue) {
              continue;
            } else {              
              return false;
            }
          }
          return true;
        };
        //std::cout << "previous size: " << (row-1)->second.size() << " now size: " << row->second.size() << std::endl;
        if (row == sorted_table_.begin()) {
          rank_value.aggregates_ = {ValueFactory::GetBooleanValue(false)};
        } else {
          if (IsDuplicate((row-1)->second, row->second)) {
            rank_value.aggregates_ = {ValueFactory::GetBooleanValue(true)};
            std::cout <<"is duplicate\n";
          } else {
            rank_value.aggregates_ = {ValueFactory::GetBooleanValue(false)};
            std::cout <<"not duplicate\n";
          } 
        }
        auto res = aht_map_[window_index]->InsertCombine(window_function_key.first, /*duplicate_signature*/rank_value);
        row->first.at(window_index) = res[0];        
      } else {
        auto res = aht_map_[window_index]->InsertCombine(window_function_key.first, window_function_key.second);
        row->first.at(window_index) = res[0];
      }
    }
  }
  std::cout << "row size: " << sorted_table_.front().first.size() << std::endl;

  if (plan_->window_functions_.at(window_indeces[0]).order_by_.empty()) {
    for (auto &row : sorted_table_) {
      std::cout <<window_indeces.size() <<std::endl;
      for (uint32_t idx=0; idx != window_indeces.size(); idx++) {
        //if (!plan_->window_functions_.at(window_indeces.at(idx)).partition_by_.empty())
        //  std::cout << "window index: " << window_indeces.at(idx) << " partition key: " << key.second.group_bys_[0].ToString() <<std::endl;
        row.first.at(window_indeces.at(idx)) = aht_map_[window_indeces.at(idx)]->GetMap().at(row.third.at(idx).first).aggregates_.at(0);
      }
    }
  }
  std::cout << "row size: " << sorted_table_.front().first.size() << std::endl;
}

WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void WindowFunctionExecutor::Init() {
  child_executor_->Init();

  // init aggregate table
  for (const auto &window_function : plan_->window_functions_) {
    // std::cout << "window index: " << window_function.first << "type: " <<
    // static_cast<int>(window_function.second.type_) << std::endl;
    std::vector<AbstractExpressionRef> function = {window_function.second.function_};
    std::vector<WindowFunctionType> type = {window_function.second.type_};
    auto aht = std::make_unique<WindowFunctionHashTable>(function, type);
    aht_map_[window_function.first] = std::move(aht);
    std::cout <<"insert aht_map_ index : " << window_function.first <<std::endl;
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
