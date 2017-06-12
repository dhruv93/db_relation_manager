#pragma once

#include <exception>
#include <string>
#include "SQLParser.h"
#include "schema_tables.h"


class SQLExecError : public std::runtime_error {
public:
    explicit SQLExecError(std::string s) : runtime_error(s) {}
};


class QueryResult {
public:
    QueryResult() : column_names(nullptr), column_attributes(nullptr), rows(nullptr), message("") {}

    QueryResult(std::string message) : column_names(nullptr), column_attributes(nullptr), rows(nullptr),
                                       message(message) {}

    QueryResult(ColumnNames *column_names, ColumnAttributes *column_attributes, ValueDicts *rows, std::string message)
            : column_names(column_names), column_attributes(column_attributes), rows(rows), message(message) {}

    virtual ~QueryResult();

    ColumnNames *get_column_names() const { return column_names; }
    ColumnAttributes *get_column_attributes() const { return column_attributes; }
    ValueDicts *get_rows() const { return rows; }
    const std::string &get_message() const { return message; }
    friend std::ostream &operator<<(std::ostream &stream, const QueryResult &qres);

protected:
    ColumnNames *column_names;
    ColumnAttributes *column_attributes;
    ValueDicts *rows;
    std::string message;
};


class SQLExec {
public:
    static QueryResult *execute(const hsql::SQLStatement *statement) throw(SQLExecError);

protected:
    static Tables *tables;

    static QueryResult *create(const hsql::CreateStatement *statement);
    static QueryResult *drop(const hsql::DropStatement *statement);
    static QueryResult *show(const hsql::ShowStatement *statement);
    static QueryResult *show_tables(const hsql::ShowStatement *statement);
    static QueryResult *show_columns(const hsql::ShowStatement *statement);

    static void
    column_definition(const hsql::ColumnDefinition *col, Identifier &column_name, ColumnAttribute &column_attribute);
};
