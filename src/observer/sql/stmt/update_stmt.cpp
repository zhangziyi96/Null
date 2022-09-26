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
// Created by Wangyunlai on 2022/5/22.
//

#include "sql/stmt/update_stmt.h"
#include "common/log/log.h"
#include "storage/common/db.h"
#include "storage/common/table.h"
#include "sql/stmt/filter_stmt.h"

UpdateStmt::UpdateStmt(Table *table, const char *attribute_name, const Value *value, FilterStmt *filter_stmt)
  : table_ (table), attribute_name_(attribute_name), value_(value), filter_stmt_(filter_stmt)
{}

RC UpdateStmt::create(Db *db, const Updates &update_sql, Stmt *&stmt)
{
  const char *table_name = update_sql.relation_name;
  if (nullptr == db || nullptr == table_name) {
    LOG_WARN("invalid argument. db=%p, table_name=%p", 
             db, table_name);
    return RC::INVALID_ARGUMENT;
  }

  // check whether the table exists
  Table *table = db->find_table(table_name);
  if (nullptr == table) {
    LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  std::unordered_map<std::string, Table *> table_map;
  table_map.insert(std::pair<std::string, Table *>(std::string(table_name), table));

  FilterStmt *filter_stmt = nullptr;
  RC rc = FilterStmt::create(db, table, &table_map,
          update_sql.conditions, update_sql.condition_num, filter_stmt);
   if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create filter statement. rc=%d:%s", rc, strrc(rc));
    return rc;
  }

  // check the fields number
  const Value value = update_sql.value;
  const char *attribute_name = update_sql.attribute_name;
  

  const TableMeta &table_meta = table->table_meta();
  // check fields name
  const std::vector<FieldMeta> *field_metas = table_meta.field_metas();
  std::set<std::string> attribute_set;
  for(FieldMeta fieldmeta: *field_metas){
    attribute_set.insert(std::string(fieldmeta.name()));
  }
  if(attribute_set.find(std::string(attribute_name)) == attribute_set.end()){
    LOG_WARN("no such attribute %s", attribute_name);
    return RC::INVALID_ARGUMENT;
  }


  // for(int i = 0; i < condition_num; i++){
  //   RelAttr attr;
  //   if (conditions[i].left_is_attr == 1){
  //     attr = conditions[i].left_attr;
  //   } else if (conditions[i].right_is_attr == 1) {
  //     attr = conditions[i].right_attr;
  //   } else {
  //     LOG_WARN("invalid argument");
  //     return RC::INVALID_ARGUMENT;
  //   }
  //   if(attribute_set.find(std::string(attr.attribute_name)) == attribute_set.end()){
  //     LOG_WARN("no such attribute %s", attribute_name);
  //     return RC::INVALID_ARGUMENT;
  //   }
  // }


  // everything alright
  stmt = new UpdateStmt(table, attribute_name, &value, filter_stmt);
  return RC::SUCCESS;
}
