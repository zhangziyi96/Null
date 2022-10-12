#include "util.h"
#include <string>
#include <sstream>
#include "sql/expr/tuple.h"
void value_to_string(std::string &str, const Value &value) {
    std::stringstream os;
    if(value.type == INTS) {
        os << *(int*)(value.data);
    } else if (value.type == FLOATS) {
        os << *(float*)(value.data);
    } else if (value.type == CHARS || value.type ==DATES) {
        int len = strlen((char*)(value.data));
        for (int i = 0; i < len ; i++) {
            if (((char*)(value.data))[i] == '\0') {
                break;
            }
            os << ((char*)(value.data))[i];
        }
    }
    str = os.str();
}

std::string relAttr_to_string(const RelAttr &attr) {
    std::string str;
    if (attr.relation_name == nullptr) {
        str = attr.attribute_name;
    } else {
        str = std::string(attr.relation_name) + "." + std::string(attr.attribute_name); 
    }
    return str;
}


std::string expr_to_string(const ExpressionNode *expr) {
    std::string op[5] = {"+", "-", "*", "/", ""};

    if(expr == nullptr) return "";
    std::string str;
    if (expr->left == nullptr && expr->right == nullptr){
        if (expr->is_attr) {
            str = relAttr_to_string(expr->attr);
        } else if (expr->is_value) {
            value_to_string(str,expr->value);
        }
        return str;
    }

    std::string left_str;
    if (expr->left != nullptr) {
        if(expr->has_brace) {
            left_str = "(" + op[expr->pre_op] + expr_to_string(expr->left) + ")";
        } else {
            left_str = op[expr->pre_op] + expr_to_string(expr->left);
        }
    }

    std::string right_str;
    if (expr->right != nullptr) {
        if(expr->has_brace) {
            right_str = "(" + op[expr->pre_op] + expr_to_string(expr->right) + ")";
        } else {
            right_str = op[expr->pre_op] + expr_to_string(expr->right);
        }
    }

    return left_str + op[expr->op] + right_str;

}

RC get_field(const std::vector<Table*> tables, const RelAttr &attr, Field &field){
    Table *table = nullptr;
    if(tables.size() == 1) {
        table = tables[0];
    } else {
        if(attr.relation_name == nullptr) {
          return RC::INVALID_ARGUMENT;
        }
        for(int i = 0; i < tables.size(); i++) {
          if(strcmp(tables[i]->name(), attr.relation_name) == 0) {
            table = tables[i];
            break;
          }
        }
    }
    if(table == nullptr){
        return RC::INVALID_ARGUMENT;
    }
    const FieldMeta *field_meta = table->table_meta().field(attr.attribute_name);
    if(field_meta == nullptr) return RC::INVALID_ARGUMENT;
    field.set_table(table);
    field.set_field(field_meta);
    return RC::SUCCESS;
}

RC cell_to_value(const TupleCell &cell, Value &value){
    AttrType type = cell.attr_type();
    const char *data = cell.data();
    value.type = type;
    switch (type){
        case INTS: {
            value.data = new int;
            *(int*)(value.data) = *(int*)data;
        } break;
        case FLOATS: {
            value.data = new float;
            *(float*)(value.data) = *(float *)data;
        } break;
        case CHARS: {
            value.data = new char[cell.length() + 1];
            for(int i = 0; i < cell.length(); i++){
                *((char*)(value.data) + i) = data[i];
            }
        } break;
        default: {
            LOG_WARN("unsupported attr type: %d", type);
            return RC::INVALID_ARGUMENT;
        } break;
    }
    return RC::SUCCESS;
}

Value value_plus_value(const Value &v1, const Value &v2) {
    Value result;
    if(v1.type == INTS && v2.type == INTS) {
      result.type = INTS;
      result.data = new int;
      *(int*)(result.data) = *(int*)v1.data + *(int*)v2.data;
      return result;
    } else if(v1.type == CHARS || v2.type ==CHARS){
      //to_do
      return result;
    } else {
      result.type = FLOATS;
      float i = 0.0;
      float j = 0.0;
      if (v1.type == INTS) {
        i = (float)(*(int*)(v1.data));
      } else if(v1.type == FLOATS) {
        i = *(float*)(v1.data);
      }
      if (v2.type == INTS) {
        j = (float)(*(int*)(v2.data));
      } else if(v2.type == FLOATS) {
        j = *(float*)(v2.data);
      }
      result.data = new float;
      *(float*)(result.data) = i + j;
      return result;
    }
}

Value value_minus_value(const Value &v1, const Value &v2) {
    Value result;
    if(v1.type == INTS && v2.type == INTS) {
      result.type = INTS;
      result.data = new int;
      *(int*)(result.data) = *(int*)v1.data - *(int*)v2.data;
      return result;
    } else if(v1.type == CHARS || v2.type ==CHARS){
      //to_do
      return result;
    } else {
      result.type = FLOATS;
      float i = 0.0;
      float j = 0.0;
      if (v1.type == INTS) {
        i = (float)(*(int*)(v1.data));
      } else if(v1.type == FLOATS) {
        i = *(float*)(v1.data);
      }
      if (v2.type == INTS) {
        j = (float)(*(int*)(v2.data));
      } else if(v2.type == FLOATS) {
        j = *(float*)(v2.data);
      }
      result.data = new float;
      *(float*)(result.data) = i - j;
      return result;
    }
}

Value value_multi_value(const Value &v1, const Value &v2) {
    Value result;
    if(v1.type == INTS && v2.type == INTS) {
      result.type = INTS;
      result.data = new int;
      *(int*)(result.data) = *(int*)v1.data * *(int*)v2.data;
      return result;
    } else if(v1.type == CHARS || v2.type ==CHARS){
      //to_do
      return result;
    } else {
      result.type = FLOATS;
      float i = 0.0;
      float j = 0.0;
      if (v1.type == INTS) {
        i = (float)(*(int*)(v1.data));
      } else if(v1.type == FLOATS) {
        i = *(float*)(v1.data);
      }
      if (v2.type == INTS) {
        j = (float)(*(int*)(v2.data));
      } else if(v2.type == FLOATS) {
        j = *(float*)(v2.data);
      }
      result.data = new float;
      *(float*)(result.data) = i * j;
      return result;
    }
}

Value value_divide_value(const Value &v1, const Value &v2) {
    Value result;
    result.type = UNDEFINED;
    result.data = nullptr;
    if(v1.type == INTS && v2.type == INTS) {
      result.type = INTS;
      result.data = new int;
      if(*(int*)(v2.data) == 0){
        return result;
      }
      *(int*)(result.data) = *(int*)v1.data / *(int*)v2.data;
      return result;
    } else if(v1.type == CHARS || v2.type ==CHARS){
      //to_do
      return result;
    } else {
      result.type = FLOATS;
      float i = 0.0;
      float j = 0.0;
      if (v1.type == INTS) {
        i = (float)(*(int*)(v1.data));
      } else if(v1.type == FLOATS) {
        i = *(float*)(v1.data);
      }
      if (v2.type == INTS) {
        if(*(int*)(v2.data) == 0){
          return result;
        }
        j = (float)(*(int*)(v2.data));
      } else if(v2.type == FLOATS) {
        j = *(float*)(v2.data);
      }
      result.data = new float;
      *(float*)(result.data) = i / j;
      return result;
    }
}