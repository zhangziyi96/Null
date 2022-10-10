#include <string.h>
#include "sql/parser/parse_defs.h"
#include "../../deps/common/log/log.h"

void expr_to_string(std::string &str, const ExpressionNode &expr);
void value_to_string(std::string &str, const Value &value);

