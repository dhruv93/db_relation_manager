#pragma once

#include "BTreeNode.h"

class BTreeBase : public DbIndex {
public:
    BTreeBase(DbRelation& relation, Identifier name, ColumnNames key_columns, bool unique);
    virtual ~BTreeBase();

    virtual void create();
    virtual void drop();

    virtual void open();
    virtual void close();

    virtual Handles* lookup(ValueDict* key);

    virtual void insert(Handle handle);
    virtual void del(Handle handle);

    virtual KeyValue *tkey(const ValueDict *key) const; // pull out the key values from the ValueDict in order

    friend std::ostream &operator<<(std::ostream &stream, BTreeBase &btree);

protected:
    static const BlockID STAT = 1;
    bool closed;
    BTreeStat *stat;
    BTreeNode *root;
    HeapFile file;
    KeyProfile key_profile;

    virtual void build_key_profile();
    virtual BTreeLeafBase *_lookup(BTreeNode *node, uint height, const KeyValue* key);
    virtual Insertion _insert(BTreeNode *node, uint height, const KeyValue* key, BTreeLeafValue handle);
    virtual void split_root(Insertion insertion);
    virtual BTreeNode *find(BTreeInterior *node, uint height, const KeyValue* key);
    Handles* _range(KeyValue *tmin, KeyValue *tmax, bool return_keys);
    virtual BTreeLeafBase *make_leaf(BlockID id, bool create) = 0;
    virtual std::ostream &_dump(std::ostream &out, BlockID block_id, uint height);
};


class BTreeIndex : public BTreeBase {
public:
    BTreeIndex(DbRelation& relation, Identifier name, ColumnNames key_columns, bool unique);
    virtual ~BTreeIndex();

    virtual Handles* range(ValueDict* min_key, ValueDict* max_key);

protected:
    virtual BTreeLeafBase *make_leaf(BlockID id, bool create);
    virtual std::ostream &_dump(std::ostream &out, BlockID block_id, uint height);
};


class BTreeFile : public BTreeBase {
public:
    BTreeFile(DbRelation& relation,
              Identifier name,
              ColumnNames key_columns,
              ColumnNames non_key_column_names,
              ColumnAttributes non_key_column_attributes,
              bool unique);
    virtual ~BTreeFile();

    virtual Handles* range(KeyValue *tmin, KeyValue *tmax);
    virtual ValueDict *lookup_value(KeyValue *key);
    virtual void insert_value(ValueDict *row);

protected:
    ColumnNames non_key_column_names;
    ColumnAttributes non_key_column_attributes;

    virtual BTreeLeafBase *make_leaf(BlockID id, bool create);
};


class BTreeTable : public DbRelation {
public:
    BTreeTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes,
               const ColumnNames& primary_key);
    virtual ~BTreeTable();

    virtual void create();
    virtual void create_if_not_exists();
    virtual void drop();

    virtual void open();
    virtual void close();

    virtual Handle insert(const ValueDict* row);
    virtual Handle update(const Handle handle, const ValueDict* new_values);
    virtual void del(const Handle handle);

    virtual Handles* select();
    virtual Handles* select(const ValueDict* where);
    virtual Handles* select(Handles *current_selection, const ValueDict* where);

    virtual ValueDict* project(Handle handle);
    virtual ValueDict* project(Handle handle, const ColumnNames* column_names);
    using DbRelation::project;

protected:
    BTreeFile *index;

    virtual ValueDict* validate(const ValueDict* row) const;
    virtual bool selected(Handle handle, const ValueDict* where);
    virtual void make_range(const ValueDict *where, KeyValue *&minval, KeyValue *&maxval, ValueDict *&additional_where);
};

bool test_btree();
bool test_btable();
