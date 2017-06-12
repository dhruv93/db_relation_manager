
#include <algorithm>
#include "SQLExec.h"
#include "EvalPlan.h"

Tables* SQLExec::tables = nullptr;
Indices* SQLExec::indices = nullptr;

typedef std::vector<hsql::Expr*> exprnList;

std::ostream &operator<<(std::ostream &out, const QueryResult &qres) {
    if (qres.column_names != nullptr) {
        for (auto const &column_name: *qres.column_names)
            out << column_name << " ";
        out << std::endl << "+";
        for (uint i = 0; i < qres.column_names->size(); i++)
            out << "----------+";
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
                    case ColumnAttribute::BOOLEAN:
                        out << (value.n == 0 ? "false" : "true");
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
    if (column_names != nullptr)
        delete column_names;
    if (column_attributes != nullptr)
        delete column_attributes;
    if (rows != nullptr) {
        for (auto row: *rows)
            delete row;
        delete rows;
    }
}


QueryResult *SQLExec::execute(const hsql::SQLStatement *statement) throw(SQLExecError) {
    // initialize _tables table, if not yet present
    if (SQLExec::tables == nullptr) {
        SQLExec::tables = new Tables();
        SQLExec::indices = new Indices();
    }

    try {
        switch (statement->type()) {
            case hsql::kStmtCreate:
                return create((const hsql::CreateStatement *) statement);
            case hsql::kStmtDrop:
                return drop((const hsql::DropStatement *) statement);
            case hsql::kStmtShow:
                return show((const hsql::ShowStatement *) statement);
            case hsql::kStmtInsert:
                return insert((const hsql::InsertStatement *) statement);
            case hsql::kStmtDelete:
                return del((const hsql::DeleteStatement *) statement);
            case hsql::kStmtSelect:
                return select((const hsql::SelectStatement *) statement);
            default:
                return new QueryResult("not implemented");
        }
    } catch (DbRelationError& e) {
        throw SQLExecError(std::string("DbRelationError: ") + e.what());
    }
}

ValueDict *SQLExec::get_where_conjunction(const hsql::Expr* expr)
{
        ValueDict *where = new ValueDict();

        get_where_conjunction(expr, *where);
        return where;

}

void SQLExec::get_where_conjunction(const hsql::Expr* expr, ValueDict &where) //Function Overriding
{
    if(expr->type == hsql::kExprOperator)
    {
        if(expr->opType == hsql::Expr::SIMPLE_OP) //Base Case
        {
            Value val;
            Identifier identifier = expr->expr->name;
            switch (expr->expr2->type) {
                case hsql::kExprLiteralString : {
                     val = Value(expr->expr2->name);
                     break;
                 }
                case hsql::kExprLiteralInt : {
                     val = Value(int32_t(expr->expr2->ival));
                     break;
                 }
                 default:
                    throw DbRelationError("Other cases not yet implemented.");
                    break;
            }
            where[identifier] = Value(val);
        }
        else if (expr->opType == hsql::Expr::AND) //Recursive Calls
        {
            get_where_conjunction(expr->expr, where);
            get_where_conjunction(expr->expr2, where);
        }
    }
}


QueryResult *SQLExec::insert(const hsql::InsertStatement *statement) {
    Identifier table_name = statement->tableName;
    DbRelation& table = SQLExec::tables->get_table(table_name);

    const ColumnNames& table_columns = table.get_column_names();

    ValueDict row;
    exprnList holder = *statement->values;

    //cout << statement->columns << endl;

    for(uint i = 0; i < holder.size(); i++)
    {
        if(statement->columns == nullptr)
        {
            if(holder[i]->type == hsql::kExprLiteralString)
            {
                row[table_columns[i]] = Value( holder[i]->name );
            }
            else if(holder[i]->type == hsql::kExprLiteralInt)
            {
                row[table_columns[i]] = Value( (int32_t) holder[i]->ival );
            }
        }

        else
        {
            if(holder[i]->type == hsql::kExprLiteralString)
            {
                row[(*statement->columns)[i]] = Value( holder[i]->name );
            }
            else if(holder[i]->type == hsql::kExprLiteralInt)
            {
                row[(*statement->columns)[i]] = Value( (int32_t) holder[i]->ival );
            }
        }
    }

    Handle handle;
    IndexNames idx_names = indices->get_index_names(statement->tableName);
    	for (auto const& idx : idx_names)
    	{
    		DbIndex& index = indices->get_index(statement->tableName, idx);
    		index.insert(handle);
    	}
    table.insert(&row);
    return new QueryResult("successfully inserted 1 row into " + table_name + " and "
                            + std::to_string(idx_names.size()) +" indices");

}

QueryResult *SQLExec::del(const hsql::DeleteStatement *statement)
{
    Identifier table_name = statement->tableName;
      DbRelation& table = SQLExec::tables->get_table(table_name);

      EvalPlan* plan = new EvalPlan(table);

      if(statement->expr != nullptr){
        plan = new EvalPlan(get_where_conjunction(statement->expr),plan);
      }

      EvalPlan* optimized = plan->optimize();

      EvalPipeline pipeline = optimized->pipeline();

        Handles* handles = pipeline.second;
      for(auto const& handle: *handles)
        table.del(handle);

      u_long n = handles->size();

      delete handles;

      return new QueryResult("DELETED statement " + n);
  }

QueryResult *SQLExec::select(const hsql::SelectStatement *statement) {

    ColumnNames *cn = new ColumnNames();
    ColumnAttributes *cas = new ColumnAttributes();
    DbRelation& table = tables->get_table(statement->fromTable->name);

    EvalPlan *plan = new EvalPlan(table);

    if(statement->whereClause != nullptr)
    {
        plan = new EvalPlan(get_where_conjunction(statement->whereClause),plan);
    }

    exprnList* select_list = statement->selectList; //TYPEDEF defined at the top

    if(select_list->at(0)->type == hsql::kExprStar)
    {   //for SELECT * queries
        *cn = table.get_column_names();
        plan = new EvalPlan(EvalPlan::ProjectAll, plan); //ProjectAll
    }

    else{ //for SELECT specific cols
            for(auto const element : *select_list)
                cn->push_back(element->name);

            plan = new EvalPlan(cn, plan); //Project specific cols
    }

    EvalPlan *optimized = plan->optimize();
    ValueDicts *rows = optimized->evaluate();

    cas = table.get_column_attributes(*cn);

    std::string message = "successfully returned " + std::to_string(rows->size()) + " rows";

    return new QueryResult(cn,cas,rows,message);
}



void SQLExec::column_definition(const hsql::ColumnDefinition *col, Identifier& column_name,
                                ColumnAttribute& column_attribute) {
    column_name = col->name;
    switch (col->type) {
        case hsql::ColumnDefinition::INT:
            column_attribute.set_data_type(ColumnAttribute::INT);
            break;
        case hsql::ColumnDefinition::TEXT:
            column_attribute.set_data_type(ColumnAttribute::TEXT);
            break;
        case hsql::ColumnDefinition::DOUBLE:
        default:
            throw SQLExecError("unrecognized data type");
    }
}

QueryResult *SQLExec::create(const hsql::CreateStatement *statement) {
    switch(statement->type) {
        case hsql::CreateStatement::kTable:
            return create_table(statement);
        case hsql::CreateStatement::kIndex:
            return create_index(statement);
        default:
            return new QueryResult("Only CREATE TABLE and CREATE INDEX are implemented");
    }
}

QueryResult *SQLExec::create_table(const hsql::CreateStatement *statement) {
    Identifier table_name = statement->tableName;
    ColumnNames column_names;
    ColumnAttributes column_attributes;
    Identifier column_name;
    ColumnAttribute column_attribute;
    for (hsql::ColumnDefinition *col : *statement->columns) {
        column_definition(col, column_name, column_attribute);
        column_names.push_back(column_name);
        column_attributes.push_back(column_attribute);
    }

    // Add to schema: _tables and _columns
    ValueDict row;
    row["table_name"] = table_name;
    Handle t_handle = SQLExec::tables->insert(&row);  // Insert into _tables
    try {
        Handles c_handles;
        DbRelation& columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
        try {
            for (uint i = 0; i < column_names.size(); i++) {
                row["column_name"] = column_names[i];
                row["data_type"] = Value(column_attributes[i].get_data_type() == ColumnAttribute::INT ? "INT" : "TEXT");
                c_handles.push_back(columns.insert(&row));  // Insert into _columns
            }

            // Finally, actually create the relation
            DbRelation& table = SQLExec::tables->get_table(table_name);
            if (statement->ifNotExists)
                table.create_if_not_exists();
            else
                table.create();

        } catch (...) {
            // attempt to remove from _columns
            try {
                for (auto const &handle: c_handles)
                    columns.del(handle);
            } catch (...) {}
            throw;
        }

    } catch (...) {
        try {
            // attempt to remove from _tables
            SQLExec::tables->del(t_handle);
        } catch (...) {}
        throw;
    }
    return new QueryResult("created " + table_name);
}

QueryResult *SQLExec::create_index(const hsql::CreateStatement *statement) {
    Identifier index_name = statement->indexName;
    Identifier table_name = statement->tableName;

    // get underlying relation
    DbRelation& table = SQLExec::tables->get_table(table_name);

    // check that given columns exist in table
    const ColumnNames& table_columns = table.get_column_names();
    for (auto const& col_name: *statement->indexColumns)
        if (std::find(table_columns.begin(), table_columns.end(), col_name) == table_columns.end())
            throw SQLExecError(std::string("Column '") + col_name + "' does not exist in " + table_name);

    // insert a row for every column in index into _indices
    ValueDict row;
    row["table_name"] = Value(table_name);
    row["index_name"] = Value(index_name);
    row["index_type"] = Value(statement->indexType);
    row["is_unique"] = Value(std::string(statement->indexType) == "BTREE"); // assume HASH is non-unique -- leave uniqueness logic for another day...
    int seq = 0;
    Handles i_handles;
    try {
        for (auto const &col_name: *statement->indexColumns) {
            row["seq_in_index"] = Value(++seq);
            row["column_name"] = Value(col_name);
            i_handles.push_back(SQLExec::indices->insert(&row));
        }

        DbIndex &index = SQLExec::indices->get_index(table_name, index_name);
        index.create();

    } catch (...) {
        // attempt to remove from _indices
        try {  // if any exception happens in the reversal below, we still want to re-throw the original exception
            for (auto const &handle: i_handles)
                SQLExec::indices->del(handle);
        } catch (...) {}
        throw;  // re-throw the original exception (which should give the client some clue as to why it didn't work)
    }
    return new QueryResult("created index " + index_name);
}

// DROP ...
QueryResult *SQLExec::drop(const hsql::DropStatement *statement) {
    switch(statement->type) {
        case hsql::DropStatement::kTable:
            return drop_table(statement);
        case hsql::DropStatement::kIndex:
            return drop_index(statement);
        default:
            return new QueryResult("Only DROP TABLE and CREATE INDEX are implemented");
    }
}

QueryResult *SQLExec::drop_table(const hsql::DropStatement *statement) {
    Identifier table_name = statement->name;
    if (table_name == Tables::TABLE_NAME || table_name == Columns::TABLE_NAME)
        throw SQLExecError("cannot drop a schema table");

    ValueDict where;
    where["table_name"] = Value(table_name);

    // get the table
    DbRelation& table = SQLExec::tables->get_table(table_name);

    // remove any indices
    for (auto const& index_name: SQLExec::indices->get_index_names(table_name)) {
        DbIndex& index = SQLExec::indices->get_index(table_name, index_name);
        index.drop();  // drop the index
    }
    Handles* handles = SQLExec::indices->select(&where);
    for (auto const& handle: *handles)
        SQLExec::indices->del(handle);  // remove all rows from _indices for each index on this table
    delete handles;

    // remove from _columns schema
    DbRelation& columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
    handles = columns.select(&where);
    for (auto const& handle: *handles)
        columns.del(handle);
    delete handles;

    // remove table
    table.drop();

    // finally, remove from _tables schema
    SQLExec::tables->del(*SQLExec::tables->select(&where)->begin()); // expect only one row from select

    return new QueryResult(std::string("dropped ") + table_name);
}

QueryResult *SQLExec::drop_index(const hsql::DropStatement *statement) {
    Identifier table_name = statement->name;
    Identifier index_name = statement->indexName;

    // drop index
    DbIndex& index = SQLExec::indices->get_index(table_name, index_name);
    index.drop();

    // remove rows from _indices for this index
    ValueDict where;
    where["table_name"] = Value(table_name);
    where["index_name"] = Value(index_name);
    Handles* handles = SQLExec::indices->select(&where);
    for (auto const& handle: *handles)
        SQLExec::indices->del(handle);
    delete handles;

    return new QueryResult("dropped index " + index_name);
}

QueryResult *SQLExec::show(const hsql::ShowStatement *statement) {
    switch (statement->type) {
        case hsql::ShowStatement::kTables:
            return show_tables();
        case hsql::ShowStatement::kColumns:
            return show_columns(statement);
        case hsql::ShowStatement::kIndex:
            return show_index(statement);
        default:
            throw SQLExecError("unrecognized SHOW type");
    }
}

QueryResult *SQLExec::show_index(const hsql::ShowStatement *statement) {
    ColumnNames* column_names = new ColumnNames;
    ColumnAttributes* column_attributes = new ColumnAttributes;
    column_names->push_back("table_name");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    column_names->push_back("index_name");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    column_names->push_back("column_name");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    column_names->push_back("seq_in_index");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::INT));

    column_names->push_back("index_type");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    column_names->push_back("is_unique");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::BOOLEAN));

    ValueDict where;
    where["table_name"] = statement->tableName;
    Handles* handles = SQLExec::indices->select(&where);
    u_long n = handles->size();

    ValueDicts* rows = new ValueDicts;
    for (auto const& handle: *handles) {
        ValueDict* row = SQLExec::indices->project(handle, column_names);
        rows->push_back(row);
    }
    delete handles;
    return new QueryResult(column_names, column_attributes, rows,
                           "successfully returned " + std::to_string(n) + " rows");
}

QueryResult *SQLExec::show_tables() {
    ColumnNames* column_names = new ColumnNames;
    column_names->push_back("table_name");

    ColumnAttributes* column_attributes = new ColumnAttributes;
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    Handles* handles = SQLExec::tables->select();
    u_long n = handles->size() - 3;

    ValueDicts* rows = new ValueDicts;
    for (auto const& handle: *handles) {
        ValueDict* row = SQLExec::tables->project(handle, column_names);
        Identifier table_name = row->at("table_name").s;
        if (table_name != Tables::TABLE_NAME
            && table_name != Columns::TABLE_NAME
            && table_name != Indices::TABLE_NAME) {

            rows->push_back(row);
        }
    }
    delete handles;
    return new QueryResult(column_names, column_attributes, rows,
                           "successfully returned " + std::to_string(n) + " rows");
}

QueryResult *SQLExec::show_columns(const hsql::ShowStatement *statement) {
    DbRelation& columns = SQLExec::tables->get_table(Columns::TABLE_NAME);

    ColumnNames* column_names = new ColumnNames;
    column_names->push_back("table_name");
    column_names->push_back("column_name");
    column_names->push_back("data_type");

    ColumnAttributes* column_attributes = new ColumnAttributes;
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    ValueDict where;
    where["table_name"] = Value(statement->tableName);
    Handles* handles = columns.select(&where);
    u_long n = handles->size();

    ValueDicts* rows = new ValueDicts;
    for (auto const& handle: *handles) {
        ValueDict* row = columns.project(handle, column_names);
        rows->push_back(row);
    }
    delete handles;
    return new QueryResult(column_names, column_attributes, rows,
                           "successfully returned " + std::to_string(n) + " rows");
}
