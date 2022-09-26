#include "common/log/log.h"
#include "sql/operator/update_operator.h"
#include "storage/common/record.h"
#include "storage/common/table.h"
#include "sql/stmt/update_stmt.h"

RC UpdateOperator::open(){
    if (children_.size() != 1) {
        LOG_WARN("delete operator must has 1 child");
        return RC::INTERNAL;
    }

    Operator *child = children_[0];
    RC rc = child->open();
    if (rc != RC::SUCCESS) {
        LOG_ERROR("failed to open child operator: %s", strrc(rc));
        return rc;
    }
    Table *table = update_stmt_->table();
    while (RC::SUCCESS == (rc = child->next())) {
        Tuple *tuple = child->current_tuple();
        
        if (nullptr == tuple) {
            LOG_WARN("failed to get current record: %s", strrc(rc));
            return rc;
        }

        RowTuple *row_tuple = static_cast<RowTuple *>(tuple);
        Record &record = row_tuple->record();
        const char *attribute_name = update_stmt_->attribute_name();
        Value *value = new Value;
        memcpy(value, update_stmt_->value(), sizeof(*value));
        if(table->table_meta().field(attribute_name)->type() == DATES){
            if(value_init_date(value, (char*)value->data) != 0){
                return RC::INVALID_ARGUMENT;
            }
        }
        // const Value *value = update_stmt_->value();
        
        rc = table->update_record(nullptr, &record, attribute_name, value);
        if (rc != RC::SUCCESS) {
            LOG_WARN("failed to update record: %s", strrc(rc));
            return rc;
        }
    }
    return RC::SUCCESS;
}

RC UpdateOperator::next(){
    RC rc = RC::RECORD_EOF;
    //TODO...
    return rc;
}

RC UpdateOperator::close(){
    RC rc = RC::SUCCESS;
    children_[0]->close();
    return rc;
}

