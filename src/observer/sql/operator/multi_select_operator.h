#pragma once

#include <vector>
#include "sql/operator/operator.h"
#include "sql/operator/predicate_operator.h"
#include "storage/common/record_manager.h"
#include "rc.h"
#include "storage/common/db.h"

class Table;

class MultiSelectOperator : public Operator
{
public:
MultiSelectOperator(PredicateOperator *pred_oper)
  : pred_oper_(pred_oper)
  {}

  virtual ~MultiSelectOperator() = default;

  RC open() override;
  RC next() override;
  RC close() override;

  Tuple * current_tuple() override;

  void get_multi_tuple();
  
  void add_tuple(RowTuple *tuple){
    this->tuples_.push_back(tuple);
  }

private:
  std::vector<RowTuple *> tuples_;
  std::vector<Operator *> stack_;
  std::vector<Operator *> out_stack_;
  PredicateOperator *pred_oper_;
  CompositeTuple tuple_;
};
