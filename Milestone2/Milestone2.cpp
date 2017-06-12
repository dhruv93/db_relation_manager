#include <cstdio>
#include <cstdlib>
#include <vector>
#include <iostream>
#include <cstdint>
#include <cstring>
#include <algorithm>
#include "db_cxx.h"
#include "heap_storage.h"
#include "storage_engine.h"


////
////
////        SlottedPage
////
////
////

// Get 2-byte integer at given offset in block.
u_int16_t SlottedPage::get_n(u_int16_t offset)
{
	return *(u_int16_t*)this->address(offset);
}

// Put a 2-byte integer at given offset in block.
void SlottedPage::put_n(u_int16_t offset, u_int16_t n)
{
	*(u_int16_t*)this->address(offset) = n;
}

// Get a void* pointer into the data block.
void* SlottedPage::address(u_int16_t offset)
{
	return (void*)((char*)this->block.get_data() + offset);
}


// Get Header
void SlottedPage::get_header(u_int16_t &size, u_int16_t &loc, RecordID id)
{
    size = get_n(id * 4);
    loc =  get_n ((id * 4) + 2);
}

// Put header
void SlottedPage::put_header(RecordID id, u_int16_t size, u_int16_t loc)
{
    if (id == 0) {
		size = this->num_records;
		loc = this->end_free;
	}
	put_n(4*id, size);
	put_n(4*id + 2, loc);
}

// Has Room
bool SlottedPage::has_room(u_int16_t size)
{
    u_int16_t mem_reqd_new = (this->num_records + 1) * 4;
    u_int16_t free_mem_size = this->end_free - mem_reqd_new;
    if (size <= free_mem_size)
        return true;

    return false;
}

//slide function
void SlottedPage::slide(u_int16_t start, u_int16_t end) {

	int shift = end - start;
	if (shift == 0)
		return;

	//slide
	int size = start - (this->end_free + 1);
	char temp_array[size];
	std::memcpy(temp_array, this->address(this->end_free + 1), size);
	std::memcpy(this->address(this->end_free + 1 + shift), temp_array, size);

	//fix headers
	for (RecordID const& tempId : *(this->ids())) {
		u_int16_t size, loc;
		this->get_header(size, loc, tempId);
		if (loc <= start) {
			loc += shift;
			this->put_header(tempId, size, loc);
		}
	}
	this->end_free += shift;
	put_header();
}


// Constructor
SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new)
	: DbBlock(block, block_id, is_new)
{
    if (is_new) {
        this->num_records =0;
        this->end_free = DB_BLOCK_SZ - 1;
        this->put_header();
    }
    else {
        this->get_header(this->num_records,this->end_free);
    }
}

// Add record
RecordID SlottedPage::add(const Dbt* data) throw(DbBlockNoRoomError)  {

    if (!this->has_room(data->get_size())) {
        std::cout<<"Error: No free space for new record."<<std::endl;
		throw DbBlockNoRoomError("Error: No free space for new record.");
    }
    else {
        this->num_records += 1;
        RecordID record_id = this->num_records;
        int size = data->get_size();
        this->end_free -= size;
        int loc = this->end_free + 1;
        this->put_header();
        this->put_header(record_id, size, loc);
        //copy data
        std::memcpy(this->address(loc), data->get_data(), size);
        std::cout<<"Success: Added new Record with recordId "<<record_id<<std::endl;
        return record_id;
    }
}

// Get Record
Dbt* SlottedPage::get(RecordID record_id) {
     u_int16_t size, loc;
     this->get_header(size, loc, record_id);
     if (loc == 0) {
         return NULL;
     }
     Dbt* blk = new Dbt(this->address(loc), size);
     return blk;
}


//Put Record (Update)
void SlottedPage::put(RecordID record_id, const Dbt &data)
	throw(DbBlockNoRoomError)
{
    //get size, loc
    u_int16_t size, loc;
    this->get_header(size, loc, record_id);
    int new_size = data.get_size();

    if (new_size > size) {
        int diff = new_size - size;
        if (!this->has_room(diff)) {
            std::cout<<"Error: Not enough room in block" <<std::endl;
			throw DbBlockNoRoomError("");
        }
        else {
            //room avaiable, slide first, then copy
            this->slide(loc, loc - diff);
    		std::memcpy(this->address(loc-diff), data.get_data(), new_size);
        }
    }
    else {
        std::memcpy(this->address(loc), data.get_data(), new_size);
        this->slide(loc+new_size, loc+size);
    }
    //update header
    this->get_header(size, loc, record_id);
    this->put_header(record_id, size, loc);
}

// Delete
void SlottedPage::del(RecordID record_id) {
    u_int16_t size, loc;
    this->get_header(size, loc, record_id);
    this->put_header(record_id, 0, 0);
    this->slide(loc, loc+size);
}

// List
RecordIDs* SlottedPage::ids(void) {
    RecordIDs* list = new RecordIDs();
    for (int i=1; i<=this->num_records; i++) {
		u_int16_t size, loc;
        this->get_header(size, loc, i);
		if (loc != 0) {
			list->push_back(i);
		}
    }
    return list;
}








////
////
////
////        HeapFile
////
////
////

// DB Open
void HeapFile::db_open(uint flags) {

    if (!this->closed)
        return;

	int ret;
	this->db.set_re_len(DB_BLOCK_SZ);
    this->dbfilename = this->name + ".db";
	if ((ret = this->db.open(NULL,
	    			this->dbfilename.c_str(),
					NULL,
					DB_RECNO,
					flags,
					0664)
			) != 0)
	{
		std::cout<<"Error: Opening Database file "<<this->dbfilename<<std::endl;
		return;
	}
	this->closed = false;
	DB_BTREE_STAT stat;
	this->db.stat(NULL, &stat, DB_FAST_STAT);
	this->last = stat.bt_ndata;
}

void HeapFile::create(void) {
	this->db_open(DB_CREATE | DB_EXCL);
	SlottedPage* block = this->get_new();
	this->put(block);
}

void HeapFile::drop(void) {
	this->close();
	Db db(_DB_ENV, 0);
	char *fileName = new char[this->dbfilename.length() + 1];
	strcpy(fileName, this->dbfilename.c_str());
	db.remove(fileName, NULL, 0);
}

void HeapFile::open(void) {
    this->db_open();
}

void HeapFile::close(void) {
	this->db.close(0);
	this->closed = true;
}

// Get New Block
SlottedPage* HeapFile::get_new(void) {

	this->last += 1;
	BlockID id = this->last;
	Dbt key(&id, sizeof(id));

	//new block and allocate in db file
	char new_block[DB_BLOCK_SZ];
	std::fill(new_block, new_block + strlen(new_block), 0);
	Dbt data(new_block, sizeof(new_block));
	this->db.put(NULL, &key, &data, 0);

	return new SlottedPage(data, this->last, true);
}

// Get Block
SlottedPage* HeapFile::get(BlockID block_id)
{
	Dbt data;
	Dbt key(&block_id, sizeof(block_id));
	this->db.get(NULL, &key, &data, 0);
	SlottedPage* blk = new SlottedPage(data, block_id);
	return blk;
}

// Put Block
void HeapFile::put(DbBlock* block) {
	int id = block->get_block_id();
	Dbt key(&id, sizeof(id));
	this->db.put(NULL, &key, block->get_block(), 0);
}

// List Blocks
BlockIDs* HeapFile::block_ids() {
	BlockIDs* list = new BlockIDs();
	for (u_int32_t i = 1; i <= this->last; i++)
		list->push_back(i);
	return list;
}










////
////
////
////        HeapTable
////
////
////

// Constructor
HeapTable::HeapTable (Identifier table_name,
	ColumnNames column_names,
	ColumnAttributes column_attributes )
	: DbRelation(table_name, column_names, column_attributes), file(table_name)
{

}

// Validate
ValueDict* HeapTable::validate(const ValueDict *row)
{
	ValueDict* tuple = new ValueDict();

	for (std::string colName: this->column_names) {
		ValueDict::const_iterator dictItem = row->find(colName);
		if (dictItem != row->end())
		{
		  // Element in vector.
		  (*tuple)[colName] = dictItem->second;
		}
		else {
			throw DbRelationError("Incorrect Value");
		}
	}
	return tuple;
}

// Append
Handle HeapTable::append(const ValueDict* row) {
	//serialize
	Dbt* data = this->marshal(row);
	SlottedPage* block = this->file.get(this->file.get_last_block_id());
	RecordID rec_id;
	try {
		rec_id = block->add(data);
	}
	catch (DbBlockNoRoomError e) {
		block = this->file.get_new();
		rec_id = block->add(data);
	}
	this->file.put(block);
	return Handle(this->file.get_last_block_id(), rec_id);
}

// Marshal (serialize)
Dbt* HeapTable::marshal(const ValueDict* row) {

	//max size of block
	char *init_data = new char[DB_BLOCK_SZ];
    int insert_idx = 0, col_idx=0;

	for (std::string colName: this->column_names) {
    	ColumnAttribute attrib = this->column_attributes[col_idx++];
		ValueDict::const_iterator dictItem = row->find(colName);
		Value value = dictItem->second;
		switch(attrib.get_data_type()) {
			case ColumnAttribute::DataType::INT: {
				*(int*) (init_data + insert_idx) = value.n;
				insert_idx += sizeof(int);
				break;
			}
			case ColumnAttribute::DataType::TEXT: {
				int len = value.s.length();
				*(u_int16_t*) (init_data + insert_idx) = len;
				insert_idx += sizeof(u_int16_t);
				std::memcpy(init_data+insert_idx, value.s.c_str(), len);
				insert_idx += len;
				break;
			}
			default:
				throw DbRelationError("Not Supported");
				break;
		}
	}	//end for

	char *final_data = new char[insert_idx];
	for (int i=0; i<insert_idx; i++) {
		final_data[i] = init_data[i];
	}
	return new Dbt(final_data, insert_idx);
}


// Unmarshal (deserialize)
ValueDict* HeapTable::unmarshal(Dbt* data) {

    ValueDict *tuple = new ValueDict();
    Value value;
    char *input_data = (char*)data->get_data();
	int idx = 0, col_idx = 0;

	for (std::string colName: this->column_names) {
    	ColumnAttribute attrib = this->column_attributes[col_idx++];
		switch(attrib.get_data_type()) {
			case ColumnAttribute::DataType::INT: {
				value.n = *(int32_t*)(input_data + idx);
	    		idx += sizeof(int32_t);
				break;
			}
			case ColumnAttribute::DataType::TEXT: {
				int len = *(int*)(input_data + idx);
	    		idx += sizeof(u_int16_t);
	    		char temp[DB_BLOCK_SZ];
				std::fill(temp, temp + DB_BLOCK_SZ, '\0');
	    		std::memcpy(temp, input_data+idx, len);
	    		value.s = std::string(temp);
	            idx += len;
				break;
			}
			default:
				throw DbRelationError("Unsupported");
				break;
		}
		(*tuple)[colName] = value;
    }
    return tuple;
}


// Create
void HeapTable::create()
{
	this->file.create();
}

//Create if not exists
void HeapTable::create_if_not_exists()
{
	try {
		this->open();
	}
	catch (DbException e) {
		this->create();
	}
}


// open
void HeapTable::open()
{
	this->file.open();
}

// drop
void HeapTable::drop()
{
	this->file.drop();
}

//close
void HeapTable::close()
{
	this->file.close();
}

// insert
Handle HeapTable::insert(const ValueDict* row)
{
	this->open();
	return (this->append(this->validate(row)));
}

// empty select
Handles* HeapTable::select()
{
	Handles* vals = new Handles();
	for (BlockID block_id : *(this->file.block_ids()))
		for (RecordID record_id : *(this->file.get(block_id)->ids()))
			vals->push_back(Handle(block_id, record_id));
	return vals;
}


// parameterized select
Handles* HeapTable::select(const ValueDict* where)
{
	//ignore all clauses
	return (this->select());
}

// project
ValueDict* HeapTable::project(Handle handle)
{
	SlottedPage* block = this->file.get(handle.first);
	Dbt* data = block->get(handle.second);
	return (unmarshal(data));
}


//project
ValueDict* HeapTable::project(Handle handle, const ColumnNames* column_names)
{
	ValueDict* row = this->project(handle);
	if (column_names->empty())
		return row;

	ValueDict* vals = new ValueDict();
	for(std::string colName : *column_names)
		(*vals)[colName] = (*row)[colName];

	return vals;
}
