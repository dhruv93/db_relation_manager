#pragma once

#include "storage_engine.h"
#include "heap_storage.h"
#include <memory.h>

typedef std::vector<ColumnAttribute::DataType> KeyProfile;
typedef std::vector<BlockID> BlockPointers;
typedef std::pair<BlockID,KeyValue> Insertion;


class BTreeNode {
public:
    BTreeNode(HeapFile &file, BlockID block_id, const KeyProfile& key_profile, bool create);
    virtual ~BTreeNode();

    static bool insertion_is_none(Insertion insertion) { return insertion.first == 0; }
    static Insertion insertion_none() { return Insertion(0, KeyValue()); }

    virtual void save();

    BlockID get_id() const { return this->id; }

    friend std::ostream &operator<<(std::ostream &stream, const BTreeNode *node);

protected:
    SlottedPage *block;
    HeapFile &file;
    BlockID id;
    const KeyProfile& key_profile;

    static Dbt *marshal_block_id(BlockID block_id);
    static Dbt *marshal_handle(Handle handle);
    virtual Dbt *marshal_key(const KeyValue *key);

    virtual BlockID get_block_id(RecordID record_id) const;
    virtual Handle get_handle(RecordID record_id) const;
    virtual KeyValue* get_key(RecordID record_id) const;

    virtual std::ostream& dump_key(std::ostream& out, const KeyValue *key) const;
};


class BTreeStat : public BTreeNode {
public:
    static const RecordID ROOT = 1;  // where we store the root id in the stat block
    static const RecordID HEIGHT = ROOT + 1;  // where we store the height in the stat block

    BTreeStat(HeapFile &file, BlockID stat_id, BlockID new_root, const KeyProfile& key_profile);
    BTreeStat(HeapFile &file, BlockID stat_id, const KeyProfile& key_profile);
    virtual ~BTreeStat() {}

    virtual void save();

    BlockID get_root_id() const { return this->root_id; }
    void set_root_id(BlockID root_id) { this->root_id = root_id; }
    uint get_height() const { return this->height; }
    void set_height(uint height) { this->height = height; }

    friend std::ostream &operator<<(std::ostream &stream, const BTreeStat *node);

protected:
    BlockID root_id;
    uint height;

};


class BTreeInterior : public BTreeNode {
public:
    BTreeInterior(HeapFile &file, BlockID block_id, const KeyProfile& key_profile, bool create);
    virtual ~BTreeInterior();

    BlockID find(const KeyValue* key) const;
    Insertion insert(const KeyValue* boundary, BlockID block_id);
    virtual void save();

    void set_first(BlockID first) { this->first = first; }
    BlockID get_first() const { return this->first; }
    const BlockPointers &get_pointers() const { return this->pointers; }

    friend std::ostream &operator<<(std::ostream &stream, const BTreeInterior *node);

protected:
    BlockID first;
    BlockPointers pointers;
    KeyValues boundaries;
};


class BTreeLeafValue {
public:
    Handle h;
    ValueDict *vd;

    BTreeLeafValue() : h(0,0), vd(nullptr) {}
    BTreeLeafValue(Handle h) : h(h), vd(nullptr) {}
    BTreeLeafValue(ValueDict *vd) : h(0,0), vd(vd) {}
    ~BTreeLeafValue() {}
};


typedef std::map<KeyValue,BTreeLeafValue> LeafMap;

class BTreeLeafBase : public BTreeNode {
public:
    BTreeLeafBase(HeapFile &file, BlockID block_id, const KeyProfile& key_profile, bool create);
    virtual ~BTreeLeafBase();

    BTreeLeafValue find_eq(const KeyValue* key) const;  // throws if not found
    Insertion insert(const KeyValue* key, BTreeLeafValue value);
    virtual void save();

    virtual Insertion split(BTreeLeafBase *new_leaf, const KeyValue* key, BTreeLeafValue value);
    virtual void del(const KeyValue* key);

    virtual LeafMap const& get_key_map() const { return this->key_map; }
    virtual BlockID get_next_leaf() const { return this->next_leaf; }

protected:
    BlockID next_leaf;
    LeafMap key_map;

    virtual BTreeLeafValue get_value(RecordID record_id) = 0;
    virtual Dbt *marshal_value(BTreeLeafValue value) = 0;
};


class BTreeLeafIndex : public BTreeLeafBase {
public:
    BTreeLeafIndex(HeapFile &file, BlockID block_id, const KeyProfile& key_profile, bool create);
    virtual ~BTreeLeafIndex();

    friend std::ostream &operator<<(std::ostream &stream, const BTreeLeafIndex *node);

protected:
    virtual BTreeLeafValue get_value(RecordID record_id);
    virtual Dbt *marshal_value(BTreeLeafValue value);
};


class BTreeLeafFile : public BTreeLeafBase {
public:
    BTreeLeafFile(HeapFile &file,
                  BlockID block_id,
                  const KeyProfile& key_profile,
                  ColumnNames non_indexed_column_names,
                  ColumnAttributes column_attributes,
                  bool create);
    virtual ~BTreeLeafFile();

protected:
    ColumnNames column_names;
    ColumnAttributes column_attributes;

    virtual BTreeLeafValue get_value(RecordID record_id);
    virtual Dbt *marshal_value(BTreeLeafValue value);
};
