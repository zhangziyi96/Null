#include "common/log/log.h"
#include "sql/operator/aggregation_operator.h"
#include "storage/common/record.h"
#include "storage/common/table.h"
#include "sql/parser/parse_defs.h"
#include <math.h>
#include <cfloat>




RC aggre_helper(Table *table, Tuple *tuple, std::vector<Aggregation> aggregations, std::vector<AggreResult> &aggre_results){
  RC rc = RC::SUCCESS;    
  for(int i = 0; i < aggregations.size(); i++) {
    Aggregation aggregation = aggregations[i];
    AggreResult &aggre_result = aggre_results[i];
    const FieldMeta *field_meta = table->table_meta().field(aggregation.attr.attribute_name);
    if(strcmp(aggregation.attr.attribute_name, "*") == 0 && aggregation.type == COUNT){

    } else if(field_meta == nullptr){
        return RC::SCHEMA_FIELD_MISSING;
    }
    
    Field field = Field(table, field_meta);

    if(aggregation.type == COUNT){        
        aggre_result.count++;
    } else if(aggregation.type == AVG){
        aggre_result.count++;
        TupleCell cell;
        tuple->find_cell(field, cell);
        if (cell.attr_type() == INTS) {
            int sum = *(int*)aggre_result.sum.data;
            sum += *(int*)cell.data();
            *(int*)(aggre_result.sum.data) = sum;
            aggre_result.avg = sum / float(aggre_result.count);
        } else if (cell.attr_type() == FLOATS) {
            float sum = *(float*)aggre_result.sum.data;
            sum += *(float*)cell.data();
            *(float*)(aggre_result.sum.data) = sum;
            aggre_result.avg = sum / float(aggre_result.count);
        }
        
    } else if (aggregation.type == MIN) {
        TupleCell cell;
        tuple->find_cell(field, cell);
        if (aggre_result.result.data == nullptr){
            aggre_result.result.data = (void*)(cell.data());
            aggre_result.char_length = cell.length();
        }

        const int compare = cell.compare(aggre_result);
        if (compare < 0) {
            aggre_result.result.data = (void*)(cell.data());
            aggre_result.char_length = cell.length();
        }
    } else if (aggregation.type == MAX) {
        TupleCell cell;
        tuple->find_cell(field, cell);
        if (aggre_result.result.data == nullptr){
            aggre_result.result.data = (void*)(cell.data());
            aggre_result.char_length = cell.length();
        }

        const int compare = cell.compare(aggre_result);
        if (compare > 0) {
            aggre_result.result.data = (void*)(cell.data());
            aggre_result.char_length = cell.length();
        }
    } 
  }
  return rc;
}

RC AggregationOperator::open()
{
  if (children_.size() != 1) {
    LOG_WARN("project operator must has 1 child");
    return RC::INTERNAL;
  }

  Operator *child = children_[0];
  RC rc = child->open();
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }
  for(int i = 0; i < aggregations_.size(); i++){
    Aggregation aggregation = aggregations_[i];
    AggreResult aggre_result;
    const FieldMeta *field_meta = table_->table_meta().field(aggregation.attr.attribute_name);
    
    //check    
    if((strcmp(aggregation.attr.attribute_name, "*") == 0 && aggregation.type == COUNT) ||
    (field_meta != nullptr && aggregation.type == COUNT)){
        aggre_result.count = 0;
        aggre_results_.push_back(aggre_result);
        continue;
    } else if(field_meta == nullptr){
        return RC::SCHEMA_FIELD_MISSING;
    } 
    const AttrType type = field_meta->type();
    if((type == CHARS || type == DATES) && aggregation.type == AVG){
        return RC::SCHEMA_FIELD_MISSING;
    }
    Value sum;
    Value result;
    sum.type = type;
    result.type = type;
    result.type = type;

    if (aggregation.type == AVG) {
        if (type == INTS) {
            sum.data = new int;
            *((int*)(sum.data)) = 0;
        } else if(type == FLOATS) {
            sum.data = new float;
            *((float*)(sum.data)) = 0;
        }
    } 

    aggre_result.count = 0;
    aggre_result.sum = sum;
    aggre_result.result = result;
    aggre_results_.push_back(aggre_result);
  }


  return RC::SUCCESS;
}

RC AggregationOperator::next()
{
  RC rc = RC::SUCCESS;
  Operator *oper = children_[0];
  while(RC::SUCCESS == (rc = oper->next())) {
    Tuple *tuple = oper->current_tuple();
    aggre_helper(table_, tuple, aggregations_, aggre_results_);
    
  }
  return rc;
}

RC AggregationOperator::close()
{
  children_[0]->close();
  return RC::SUCCESS;
}
Tuple *AggregationOperator::current_tuple()
{
  tuple_.set_tuple(children_[0]->current_tuple());
  return &tuple_;
}
