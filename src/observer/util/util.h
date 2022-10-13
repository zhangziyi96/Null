#pragma once

#include <string.h>
#include "sql/parser/parse_defs.h"
#include "../../deps/common/log/log.h"
#include "rc.h"
#include "storage/common/db.h"
#include "sql/expr/tuple.h"
#include "storage/common/field.h"

std::string expr_to_string(const ExpressionNode *expr);
void value_to_string(std::string &str, const Value &value);
std::string relAttr_to_string(const RelAttr &attr);

RC get_field(const std::vector<Table*> tables, const RelAttr &attr, Field &field);

RC cell_to_value(const TupleCell &cell, Value &value);

Value value_plus_value(const Value &v1, const Value &v2);
Value value_minus_value(const Value &v1, const Value &v2);
Value value_multi_value(const Value &v1, const Value &v2);
Value value_divide_value(const Value &v1, const Value &v2);

Value cal_expr_value(const ExpressionNode &expr, const Tuple &tuple, const std::vector<Table*> tables);

void cell_set_value(const Value &value, int length, TupleCell &cell);