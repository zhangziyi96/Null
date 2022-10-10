#include "util.h"
#include <string>
#include <sstream>
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