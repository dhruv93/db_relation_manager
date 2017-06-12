/**
 * Storage engine abstract classes.
 * DbBlock
 * DbFile
 * DbRelation
 *
 * @author Kevin Lundeen
 * @version 1.0, 04/04/2017
 * For CPSC4300/5300 S17, Seattle University
 *
 */
#pragma once

#include <exception>
#include <map>
#include <utility>
#include <vector>
#include "db_cxx.h"

extern DbEnv* _DB_ENV;
const uint DB_BLOCK_SZ = 4096;
typedef uint16_t RecordID;
typedef uint32_t BlockID;
typedef std::vector<RecordID> RecordIDs;
typedef std::length_error DbBlockNoRoomError;

class DbBlock {
public:
	DbBlock(Dbt &block, BlockID block_id, bool is_new=false) : block(block), block_id(block_id) {}
	virtual ~DbBlock() {}

	virtual RecordID add(const Dbt* data) throw(DbBlockNoRoomError) = 0;
	virtual Dbt* get(RecordID record_id) const = 0;
	virtual void put(RecordID record_id, const Dbt &data) throw(DbBlockNoRoomError) = 0;
	virtual void del(RecordID record_id) = 0;
	virtual RecordIDs* ids() const = 0;

	virtual Dbt* get_block() {return &block;}
	virtual void* get_data() {return block.get_data();}
	virtual BlockID get_block_id() {return block_id;}

protected:
	Dbt block;
	BlockID block_id;
};

typedef std::vector<BlockID> BlockIDs;  // FIXME: will need to turn this into an iterator at some point

class DbFile {
public:
	DbFile(std::string name) : name(name) {}
	virtual ~DbFile() {}

	virtual void create() = 0;
	virtual void drop() = 0;
	virtual void open() = 0;
	virtual void close() = 0;
	virtual DbBlock* get_new() = 0;
	virtual DbBlock* get(BlockID block_id) = 0;
	virtual void put(DbBlock* block) = 0;
	virtual BlockIDs* block_ids() const = 0;

protected:
	std::string name;
};

class ColumnAttribute {
public:
	enum DataType {
		INT,
		TEXT,
        BOOLEAN
	};
    ColumnAttribute() : data_type(INT) {}
	ColumnAttribute(DataType data_type) : data_type(data_type) {}
	virtual ~ColumnAttribute() {}

	virtual DataType get_data_type() { return data_type; }
	virtual void set_data_type(DataType data_type) {this->data_type = data_type;}

protected:
	DataType data_type;
};

class Value {
public:
	ColumnAttribute::DataType data_type;
	int32_t n;
	std::string s;

	Value() : n(0) {data_type = ColumnAttribute::INT;}
	Value(int32_t n) : n(n) {data_type = ColumnAttribute::INT;}
	Value(std::string s) : s(s) {data_type = ColumnAttribute::TEXT; }
    Value(const char *s) : s(s) {data_type = ColumnAttribute::TEXT; }
    Value(bool b) : n(b ? 1: 0) {data_type = ColumnAttribute::BOOLEAN; }

	bool operator==(const Value &other) const;
    bool operator!=(const Value &other) const;
};

typedef std::string Identifier;
typedef std::vector<Identifier> ColumnNames;
typedef std::vector<ColumnAttribute> ColumnAttributes;
typedef std::pair<BlockID, RecordID> Handle;
typedef std::vector<Handle> Handles;  // FIXME: will need to turn this into an iterator at some point
typedef std::map<Identifier, Value> ValueDict;
typedef std::vector<ValueDict*> ValueDicts;

class DbRelationError : public std::runtime_error {
public:
	explicit DbRelationError(std::string s) : runtime_error(s) {}
};

class DbRelation {
public:
	DbRelation(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes ) :
		table_name(table_name), column_names(column_names), column_attributes(column_attributes) {}
	virtual ~DbRelation() {}

	virtual void create() = 0;
	virtual void create_if_not_exists() = 0;
	virtual void drop() = 0;

	virtual void open() = 0;
	virtual void close() = 0;

	virtual Handle insert(const ValueDict* row) = 0;
	virtual void update(const Handle handle, const ValueDict* new_values) = 0;
	virtual void del(const Handle handle) = 0;

	virtual Handles* select() = 0;
	virtual Handles* select(const ValueDict* where) = 0;
    virtual Handles* select(Handles* current_selection, const ValueDict* where) = 0;

	virtual ValueDict* project(Handle handle) = 0;
	virtual ValueDict* project(Handle handle, const ColumnNames* column_names) = 0;
    virtual ValueDict* project(Handle handle, const ValueDict* column_names_from_dict);
    virtual ValueDicts* project(Handles *handles);
    virtual ValueDicts* project(Handles *handles, const ColumnNames* column_names);
    virtual ValueDicts* project(Handles *handles, const ValueDict* column_names);

    virtual const ColumnNames& get_column_names() const { return column_names; }
    virtual const ColumnAttributes get_column_attributes() const { return column_attributes; }
    virtual ColumnAttributes* get_column_attributes(const ColumnNames &select_column_names) const;

protected:
	Identifier table_name;
	ColumnNames column_names;
	ColumnAttributes column_attributes;
};

class DbIndex {
public:
    static const uint MAX_COMPOSITE = 32U; // maximum number of columns in a composite index

    DbIndex(DbRelation& relation, Identifier name, ColumnNames key_columns, bool unique)
            : relation(relation), name(name), key_columns(key_columns), unique(unique) {}
    virtual ~DbIndex() {}

    virtual void create() = 0;
    virtual void drop() = 0;

    virtual void open() = 0;
    virtual void close() = 0;

    virtual Handles* lookup(ValueDict* key_values) const = 0;
    virtual Handles* range(ValueDict* min_key, ValueDict* max_key) const {
        throw DbRelationError("range index query not supported");
    }

    virtual void insert(Handle handle) = 0;
    virtual void del(Handle handle) = 0;

protected:
    DbRelation& relation;
    Identifier name;
    ColumnNames key_columns;
    bool unique;
};
