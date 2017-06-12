#pragma once

#include "heap_storage.h"


void initialize_schema_tables();
class Columns; // forward declare

// The table that stores the metadata for all other tables.
// For now, we are not indexing anything, so a query requires sequential scan of table.
class Tables : public HeapTable {
public:
    static const Identifier TABLE_NAME;
protected:
    static ColumnNames& COLUMN_NAMES();
    static ColumnAttributes& COLUMN_ATTRIBUTES();
    static Columns* columns_table;

public:
    static void get_columns(Identifier table_name,
                                    ColumnNames &column_names, ColumnAttributes &column_attributes);
    static DbRelation& get_table(Identifier table_name);

    Tables();
    virtual ~Tables() {}

    virtual void create();
    virtual Handle insert(const ValueDict* row);
    virtual void del(Handle handle);


private:
    static std::map<Identifier,DbRelation*> table_cache;
};


class Columns : public HeapTable {
public:
    static const Identifier TABLE_NAME;
protected:
    static ColumnNames& COLUMN_NAMES();
    static ColumnAttributes& COLUMN_ATTRIBUTES();

public:
    Columns();
    virtual ~Columns() {}

    virtual void create();
    virtual Handle insert(const ValueDict* row);
};


typedef ColumnNames IndexNames;

class Indices : public HeapTable {
public:
    static const Identifier TABLE_NAME;
protected:
    static ColumnNames& COLUMN_NAMES();
    static ColumnAttributes& COLUMN_ATTRIBUTES();

public:
    virtual void get_columns(Identifier table_name, Identifier index_name,
                             ColumnNames &column_names, bool &is_hash, bool &is_unique);
    virtual DbIndex& get_index(Identifier table_name, Identifier index_name);
    virtual IndexNames get_index_names(Identifier table_name);

    Indices();
    virtual ~Indices() {}

    virtual Handle insert(const ValueDict* row);
    virtual void del(Handle handle);

private:
    static std::map<std::pair<Identifier,Identifier>,DbIndex*> index_cache;
};
