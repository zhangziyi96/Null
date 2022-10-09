/* Copyright (c) 2021 Xie Meiyi(xiemeiyi@hust.edu.cn) and OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by WangYunlai on 2022/07/01.
//

#include "common/log/log.h"
#include "sql/operator/multi_select_operator.h"
#include "storage/common/record.h"
#include "storage/common/table.h"
#include <string.h>
#include <stdio.h>

RC MultiSelectOperator::open()
{

  RC rc = RC::SUCCESS;
  for (Operator *oper: children_){
    rc = oper->open();
    if(rc != RC::SUCCESS){
        return rc;
    }
    out_stack_.push_back(oper);
    
  }
  std::reverse(out_stack_.begin(), out_stack_.end());
  out_stack_.pop_back();
  stack_.push_back(children_[0]);

  return rc;
}

RC MultiSelectOperator::next()
{
    RC rc = RC::SUCCESS;
    while(!stack_.empty()){
        Operator *stack_oper = stack_[stack_.size() - 1];
        if(RC::SUCCESS == (rc = stack_oper->next())){
            add_tuple(static_cast<RowTuple*>(stack_oper->current_tuple()));
            tuple_.clear();
            get_multi_tuple();
            if (!pred_oper_->do_predicate(static_cast<RowTuple &>(*(current_tuple())))) {
              tuples_.pop_back();
              return RC::FILTER_FAIL;
            }
            while(!out_stack_.empty()){
                Operator *out_oper = out_stack_[out_stack_.size() - 1];
                if(RC::SUCCESS == (rc = out_oper->next())){
                    add_tuple(static_cast<RowTuple*>(out_oper->current_tuple()));
                    tuple_.clear();
                    get_multi_tuple();
                    if (!pred_oper_->do_predicate(static_cast<RowTuple &>(*(current_tuple())))) {
                      tuples_.pop_back();
                      stack_.push_back(out_stack_[out_stack_.size() - 1]);
                      out_stack_.pop_back();
                      return RC::FILTER_FAIL;
                    }
                }
                stack_.push_back(out_stack_[out_stack_.size() - 1]);
                out_stack_.pop_back();
            }
            tuple_.clear();
            get_multi_tuple();
            tuples_.pop_back();
            return RC::SUCCESS;
        } else {
            stack_.pop_back();
            stack_oper->close();
            stack_oper->open();
            out_stack_.push_back(stack_oper);
            if(!tuples_.empty())
              tuples_.pop_back();
             
        }

    }
    return RC::RECORD_EOF;
}

void MultiSelectOperator::get_multi_tuple(){
  for(auto *tuple: tuples_){
    tuple_.push_back(tuple);
  }
}

// RC MultiSelectOperator::merge_tuple(){
//     char *merge_record_date = new char[temp_table_->table_meta().record_size()];
//     int offset = 4;
//     for (RowTuple *tuple: tuples_){
//         const char *record_date = tuple->record().data();
//         const Table *table = tuple->table();
//         int sys_fields_offset = table->table_meta().sys_fields_offset();
//         int copy_len = table->table_meta().record_size() - 4;
        
//         memcpy(merge_record_date + offset, record_date + 4, copy_len);
//         offset += copy_len;
//     }
//     Record *record = new Record;
//     record->set_data(merge_record_date);
//     tuple_.set_record(record);
//     return RC::SUCCESS;
// }

RC MultiSelectOperator::close()
{
  RC rc = RC::SUCCESS;
  for (Operator *oper: children_){
    rc = oper->close();
    if(rc != RC::SUCCESS){
        return rc;
    }
  }
  return rc;
}

Tuple * MultiSelectOperator::current_tuple()
{
  // LOG_ERROR("tuple %s, %d, %d, %d", tuple_.table()->name(), *(int*)(tuple_.record().data() + 4), *(int*)(tuple_.record().data() + 8), *(int*)(tuple_.record().data() + 12));

  return &tuple_;
}