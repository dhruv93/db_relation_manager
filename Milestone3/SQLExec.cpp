//
// Created by Kevin Lundeen on 4/6/17.
//

#include "SQLExec.h"


Tables* SQLExec::tables = nullptr;

std::ostream &operator<<(std::ostream &out, const QueryResult &qres) {
    if (qres.column_names != nullptr) {
        for (auto const &column_name: *qres.column_names)
            out << column_name << " ";
        out << std::endl << "+";
        for (u_long i = 0; i < qres.column_names->size(); i++)
            out << "------------+";
        out << std::endl;
        for (auto const &row: *qres.rows) {
            for (auto const &column_name: *qres.column_names) {
                Value value = row->at(column_name);
                switch (value.data_type) {
                    case ColumnAttribute::INT:
                        out << value.n;
                        break;
                    case ColumnAttribute::TEXT:
                        out << "\"" << value.s << "\"";
                        break;
                    default:
                        out << "???";
                }
                out << " ";
            }
            out << std::endl;
        }
    }
    out << qres.message;
    return out;
}

QueryResult::~QueryResult() {
 }

QueryResult *SQLExec::execute(const hsql::SQLStatement *statement) throw(SQLExecError) {
    try {
        //Initialize _tables table, if not yet present
        if(SQLExec::tables==nullptr)
            SQLExec::tables = new Tables();
        switch (statement->type()) {
            case hsql::kStmtCreate:
                return create((const hsql::CreateStatement *) statement);
            case hsql::kStmtDrop:
                return drop((const hsql::DropStatement *) statement);
            case hsql::kStmtShow:
                return show((const hsql::ShowStatement *) statement);
            default:
                return new QueryResult("not implemented");
        }
    } catch (DbRelationError& e) {
        throw SQLExecError(std::string("DbRelationError: ") + e.what());
    }
}


/*
****************************************
*   CREATE TABLE
****************************************
*/



QueryResult *SQLExec::create(const hsql::CreateStatement *statement) {

    if(statement->type!= hsql::CreateStatement::kTable)
        return new QueryResult("Only CREATE TABLE implemented");

    ValueDict* row = new ValueDict();
    Handle table_handle;
    Handles column_handles;
    ColumnNames cn;

    ColumnAttributes cas;
    ColumnAttribute ca;
    Columns& column_table = (Columns&)  SQLExec::tables->get_table(Columns::TABLE_NAME);
    std::string new_table = statement->tableName;

    (*row)["table_name"] = std::string(new_table);
    table_handle = SQLExec::tables->insert(row);
    try {
        for(auto const& column : *statement->columns)
        {
            (*row)["column_name"] = Value(column->name);
            cn.push_back(column->name);

            if(column->type==hsql::ColumnDefinition::DataType::INT) {
                (*row)["data_type"] = Value("INT");
                ca.set_data_type(ColumnAttribute::INT);
            }

            else if(column->type==hsql::ColumnDefinition::DataType::TEXT) {
                (*row)["data_type"] = Value("TEXT");
                ca.set_data_type(ColumnAttribute::TEXT);
            }

            column_handles.push_back(column_table.insert(row));
            cas.push_back(ca);
        }
    } catch(DbRelationError& e) {
        for(auto const& colHandle:column_handles)
            column_table.del(colHandle);
        SQLExec::tables->del(table_handle);
        throw(e);
    }
    HeapTable newTable(new_table,cn,cas);
    newTable.create();
    return new QueryResult("created " + new_table);
}




/*
****************************************
*   DROP TABLE
****************************************
*/

QueryResult *SQLExec::drop(const hsql::DropStatement *statement) {

try{
    HeapTable& dropTable = (HeapTable&)SQLExec::tables->get_table(statement->name);
    dropTable.drop();
}catch(DbRelationError& err){
    throw(err);
}

HeapTable& del_table = (HeapTable&)SQLExec::tables->get_table(Tables::TABLE_NAME);
ValueDict row;
row["table_name"] = Value(statement->name);
Handles deleteTable_handle = *del_table.select(&row);
for(auto const& delTable: deleteTable_handle)
    tables->del(delTable);

HeapTable& del_column = (HeapTable&)SQLExec::tables->get_table(Columns::TABLE_NAME);

Handles deleteColumn_handle = *del_column.select(&row);
for(auto const& delColumn: deleteColumn_handle)
    del_column.del(delColumn);
std::string message = "Table dropped";
return new QueryResult(message);
}





/*
****************************************
*   SHOW TABLE
****************************************
*/

QueryResult *SQLExec::show(const hsql::ShowStatement *statement)  {
    ColumnNames *column_names = new ColumnNames();
    ColumnAttributes *column_attributes = new ColumnAttributes();
    ValueDicts *rows = new ValueDicts();
    std::string message;

switch(statement->type) {

    case hsql::ShowStatement::kTables:   //SHOW TABLES
    {

    tables->get_columns(Tables::TABLE_NAME, *column_names, *column_attributes);

    HeapTable &table = (HeapTable&)tables->get_table(Tables::TABLE_NAME);
    Handles* handles = table.select();

    for(auto const& handle: *handles)
    {
        ValueDict* row = table.project(handle);
        Value val = (*row)["table_name"];
    if(val.s != Tables::TABLE_NAME && val.s != Columns::TABLE_NAME)
        rows->push_back(row);
    }
    message = "successfully returned " + std::to_string(rows->size()) + " rows";

    return new QueryResult(column_names, column_attributes, rows, message);
    }

    case hsql::ShowStatement::kColumns:    //SHOW COLUMNS
    {
    HeapTable &column_table = (HeapTable&)tables->get_table(Columns::TABLE_NAME);
    tables->get_columns(Columns::TABLE_NAME, *column_names, *column_attributes);
    Handles* handles = column_table.select();

    for(auto const& handle: *handles) {
        ValueDict* row = column_table.project(handle);
        Value val = (*row)["table_name"];
        if(val.s==statement->tableName)
            rows->push_back(row);
        }

    message = "successfully returned " + std::to_string(rows->size())+" rows";
    return new QueryResult(column_names, column_attributes, rows, message);

    }
    case hsql::ShowStatement::kIndex:
        return new QueryResult("Not yet implemented");
    }

}
