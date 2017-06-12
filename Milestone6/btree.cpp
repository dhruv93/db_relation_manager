
#include <stdio.h>
#include "btree.h"


BTreeIndex::BTreeIndex(DbRelation& relation, Identifier name, ColumnNames key_columns, bool unique)
: DbIndex(relation, name, key_columns, unique),
    closed(true), stat(nullptr), root(nullptr), file(relation.get_table_name() + "-" + this->name)
{
    build_key_profile();
    if(!unique)
        throw DbRelationError("BTree Index must be on a unique search key");
}



BTreeIndex::~BTreeIndex()
{
    if (stat)
    {
        delete stat;
    }
    if (root)
    {
        delete root;
    }
}

void BTreeIndex::build_key_profile()
{
    ColumnAttributes *cas = new ColumnAttributes;
    cas = relation.get_column_attributes(key_columns);
    
    for(auto ca : *cas)
        key_profile.push_back(ca.get_data_type());
    
    delete cas;
}

void BTreeIndex::create()
{
    file.create();
    stat = new BTreeStat(this->file, this->STAT, this->STAT + 1, this->key_profile);
    root = new BTreeLeaf(this->file,this->stat->get_root_id(),this->key_profile,true);
    closed = false;
    
    Handles* handles = relation.select();
    Handles* new_handles = new Handles();
    
    try{
        
        for(auto const handle : *handles)
        {
            insert(handle);
            new_handles->push_back(handle);
        }
    }catch(DbRelationError)
    {
        file.drop();
        throw;
    }
    
    delete handles;
    delete new_handles;
}

void BTreeIndex::open()
{
    if(closed)
    {
        file.open();
        stat = new BTreeStat(file, STAT, key_profile);
        
        if(stat->get_height() == 1)
            root = new BTreeLeaf(file, stat->get_root_id(), key_profile, false);
        
        else
            root = new BTreeInterior(file, stat->get_root_id(), key_profile, false);
        
        closed = false;
    }
    
}
void BTreeIndex::close()
{
    file.close();
    delete stat;
    stat = nullptr;
    delete root;
    root = nullptr;
    closed = true;
}

void BTreeIndex::drop()
{
    file.drop();
}

void BTreeIndex::insert(Handle handle)
{
    KeyValue* kv = tkey(relation.project(handle, &key_columns));
    
    Insertion split_root = _insert(this->root, this->stat->get_height(), kv, handle);
    
    if(!BTreeNode::insertion_is_none(split_root))
    {
        BlockID rroot = split_root.first;
        KeyValue boundary = split_root.second;
        
        BTreeInterior *root1 = new BTreeInterior(file, 0, key_profile, true);
        
        root1->set_first(root->get_id());
        root1->insert(&boundary, rroot);
        root1->save();
        
        stat->set_root_id(root1->get_id());
        stat->set_height(stat->get_height() + 1);
        
        stat->save();
        root = root1;
    }
        
}
void BTreeIndex::del(Handle handle){
    throw DbRelationError("ERROR : DEL not yet implemented ");
}

KeyValue* BTreeIndex::tkey(const ValueDict *key) const {
    
    KeyValue* kv = new KeyValue();
    
    for( auto const k : *key)
    {
        kv->push_back(k.second);
    }
    return kv;
}

Handles* BTreeIndex::_lookup(BTreeNode* node, uint height, const KeyValue* key) const  {
    
    Handles* handles = new Handles;
    
    if (height == 1) //Base Case
    {
        try
        {
            BTreeLeaf* leaf_node = (BTreeLeaf*)node;
            handles->push_back(leaf_node->find_eq(key));
        }
        catch (std::out_of_range) {}
        
        return handles;
    }
    else    //recursive call
    {
        BTreeInterior* interior_node = (BTreeInterior*)node;
        return _lookup(interior_node->find(key, height), height - 1, key);
    }
}

Handles* BTreeIndex::lookup(ValueDict* key) const {
    
    return _lookup(root, stat->get_height(), tkey(key));
}


Handles* BTreeIndex::range(ValueDict* min_key, ValueDict* max_key) const
{
    throw DbRelationError("ERROR : RANGE not yet implemented");
}


Insertion BTreeIndex::_insert(BTreeNode* node, uint height, const KeyValue* key, Handle handle)
{
    Insertion insertion;
    
    if(height==1) //Base Case
    {
        
            BTreeLeaf* leafNode = (BTreeLeaf*)node;
            insertion = leafNode->insert(key, handle);
            leafNode->save();
            return insertion;
    }
    
    BTreeInterior* interior = (BTreeInterior*)node;
    insertion = _insert(interior->find(key, height), height - 1, key, handle); //Recursive Call
    
    if (!BTreeNode::insertion_is_none(insertion))
    {
        insertion = interior->insert(&insertion.second, insertion.first);
        interior->save();
    }
    return insertion;
}


bool test_helper(HeapTable* table, Handles* handles, ValueDicts results, const ValueDict& row)
{
    for (auto handle : *handles)
    {
        results.push_back(table->project(handle));
    }
    
    for (auto result : results)
    {
        if (row.at("a").n != result->at("a").n || row.at("b").n != result->at("b").n)
        {
            return false;
        }
    }
    return true;
}


bool test_btree(){
    
    
     ColumnNames cn = {"a", "b"};
    ColumnAttributes cas = {ColumnAttribute::DataType::INT, ColumnAttribute::DataType::INT};
    
    HeapTable table("foo",cn, cas);
    
    
    table.create(); //Creating a new table FOO with two cols A, B
    
    ValueDict row1, row2;
    
    row1["a"] = Value(12);
    row1["b"] = Value(99);
    table.insert(&row1);
    
    row2["a"] = Value(88);
    row2["b"] = Value(101);
    table.insert(&row2);
    
    ValueDict row;
    
    for(int i=0; i<1000; i++)
    {
        row["a"] = Value(100 + i);
        row["b"] = Value(-i);
        table.insert(&row);
        row.clear();
    }
    
    ColumnNames idx_cn = {"a"};
    BTreeIndex idx(table,"fooIndex",idx_cn, true);
    idx.create();
    
    ValueDict test_row;
    test_row["a"] = Value(12);
    Handles *handles = idx.lookup(&test_row);
    ValueDicts result;
    
    if(!test_helper(&table, handles, result, row1))
    {
        std::cout<<"Test failed on ROW1 (12)"<<std::endl;
        return false;
    }
    
    test_row.clear();
    handles->clear();
    result.clear();
    
    test_row["a"] = Value(88);
    handles = idx.lookup(&test_row);
    
    if(!test_helper(&table, handles, result, row2))
    {
        std::cout<<"Test failed on ROW2 (88)"<<std::endl;
        return false;
    }
    
    test_row.clear();
    handles->clear();
    result.clear();
    
    test_row["a"] = Value(6);
    handles = idx.lookup(&test_row);
    
    if(!test_helper(&table, handles, result, row))
    {
        std::cout<<"Test failed on ROW (6)"<<std::endl;
        return false;
    }

    test_row.clear();
    row.clear();
    handles->clear();
    result.clear();
    
    for(int j=0; j<10; j++)
    {
        for(int i=0;i<1000; i++)
        {
            test_row["a"] = Value(i+100);
            handles = idx.lookup(&test_row);
            row["a"] = Value(i+100);
            row["b"] = Value(-i);
            
            if(!test_helper(&table, handles, result, row))
            {
                std::cout<<"Test failed inside da loopy loop"<<std::endl;
                return false;
            }
            
            test_row.clear();
            row.clear();
            handles->clear();
            result.clear();
        }
    }
    
    idx.drop();
    table.drop();
    return true;
}
