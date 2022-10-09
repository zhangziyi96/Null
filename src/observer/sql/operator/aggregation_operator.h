#pragma once

#include "sql/operator/operator.h"
#include "rc.h"

class AggregationOperator : public Operator
{
public:
  AggregationOperator(std::vector<Aggregation> aggregations, Table *table)
    : aggregations_(aggregations), table_(table)
  {}

  virtual ~AggregationOperator() = default;

  RC open() override;
  RC next() override;
  RC close() override;

  std::vector<AggreResult> aggre_results() const {return aggre_results_; }

  std::vector<Aggregation> aggregations() const {return aggregations_; }

  int aggre_num() const{
    return aggregations_.size();
  }

  Tuple * current_tuple() override;

private:
  ProjectTuple tuple_;
  std::vector<Aggregation> aggregations_;
  std::vector<AggreResult> aggre_results_;
  Table *table_;
};
