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
// Created by WangYunlai on 2022/07/05.
//

#include "sql/expr/tuple_cell.h"
#include "storage/common/field.h"
#include "common/log/log.h"
#include "util/comparator.h"
#include "sql/parser/parse_defs.h"

void TupleCell::to_string(std::ostream &os) const
{
  switch (attr_type_) {
  case INTS: {
    os << *(int *)data_;
  } break;
  case FLOATS: {
    os << *(float *)data_;
  } break;
  case CHARS: {
    for (int i = 0; i < length_; i++) {
      if (data_[i] == '\0') {
        break;
      }
      os << data_[i];
    }
  } break;
  case DATES: {
   for (int i = 0; i < 11; i++) {
      if (data_[i] == '\0') {
        break;
      }
      os << data_[i];
    }
  }
  default: {
    LOG_WARN("unsupported attr type: %d", attr_type_);
  } break;
  }
}

int TupleCell::compare(const TupleCell &other) const
{
  if (this->attr_type_ == other.attr_type_) {
    switch (this->attr_type_) {
    case INTS: return compare_int(this->data_, other.data_);
    case FLOATS: return compare_float(this->data_, other.data_);
    case CHARS: return compare_string(this->data_, this->length_, other.data_, other.length_);
    case DATES: return compare_date(this->data_, other.data_);
    default: {
      LOG_WARN("unsupported type: %d", this->attr_type_);
    }
    }
  } else if (this->attr_type_ == INTS && other.attr_type_ == FLOATS) {
    float this_data = *(int *)data_;
    return compare_float(&this_data, other.data_);
  } else if (this->attr_type_ == FLOATS && other.attr_type_ == INTS) {
    float other_data = *(int *)other.data_;
    return compare_float(data_, &other_data);
  } else if (this->attr_type_ == DATES && other.attr_type_ == CHARS) {
    char *other_data = new char[11];
    format_date(other.data_,other_data);
    return compare_string(data_,11, other_data,11);
  }
  LOG_WARN("not supported");
  return -1; // TODO return rc?
}

int TupleCell::compare(const AggreResult &other) const{
  if (this->attr_type_ == other.result.type) {
    switch (this->attr_type_) {
    case INTS: return compare_int(this->data_, other.result.data);
    case FLOATS: return compare_float(this->data_, other.result.data);
    case CHARS: return compare_string(this->data_, this->length_, other.result.data, other.char_length);
    case DATES: return compare_date(this->data_, other.result.data);
    default: {
      LOG_WARN("unsupported type: %d", this->attr_type_);
    }
    }
  } else if (this->attr_type_ == INTS && other.result.type == FLOATS) {
    float this_data = *(int *)data_;
    return compare_float(&this_data, other.result.data);
  } else if (this->attr_type_ == FLOATS && other.result.type == INTS) {
    float other_data = *(int *)other.result.data;
    return compare_float(data_, &other_data);
  } else if (this->attr_type_ == DATES && other.result.type == CHARS) {
    char *other_data = new char[11];
    format_date((char*)(other.result.data),other_data);
    return compare_string(data_,11, other_data,11);
  }
  LOG_WARN("not supported");
  return -1; // TODO return rc?
}