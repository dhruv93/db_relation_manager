//
// Created by Kevin Lundeen on 4/6/17.
//

#pragma once
#include <string>
#include <vector>
#include "SQLParser.h"


class ParseTreeToString {
public:
    static std::string statement(const hsql::SQLStatement* statement);

    static std::string operator_expression(const hsql::Expr *expr);
    static std::string expression(const hsql::Expr *expr);
    static std::string table_ref(const hsql::TableRef *table);
    static std::string column_definition(const hsql::ColumnDefinition *col);
    static std::string select(const hsql::SelectStatement *stmt);
    static std::string insert(const hsql::InsertStatement *stmt);
    static std::string create(const hsql::CreateStatement *stmt);
    static std::string drop(const hsql::DropStatement *stmt);
    static std::string show(const hsql::ShowStatement *stmt);

    static const std::vector<std::string> reserved_words;
    static bool is_reserved_word(std::string word);
};
