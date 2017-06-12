//
// Created by Kevin Lundeen on 4/24/17.
//

#include "EvalPlan.h"
#include "SQLExec.h" // for SQLExec::indices


class Dummy : public DbRelation {
public:
    static Dummy& one() {static Dummy d; return d;}
    Dummy() : DbRelation("dummy", ColumnNames(), ColumnAttributes()) {}
    virtual void create() {};
    virtual void create_if_not_exists() {};
    virtual void drop() {};
    virtual void open() {};
    virtual void close() {};
    virtual Handle insert(const ValueDict* row) {return Handle();}
    virtual Handle update(const Handle handle, const ValueDict* new_values) { return Handle(); }
    virtual void del(const Handle handle) {}
    virtual Handles* select() {return nullptr;};
    virtual Handles* select(const ValueDict* where) {return nullptr;}
    virtual Handles* select(Handles* current_selection, const ValueDict* where) {return nullptr;}
    virtual ValueDict* project(Handle handle) {return nullptr;}
    virtual ValueDict* project(Handle handle, const ColumnNames* column_names) {return nullptr;}
};

EvalPlan::EvalPlan(PlanType type, EvalPlan *relation)
        : type(type),
          relation(relation),
          projection(nullptr),
          select_conjunction(nullptr),
          table(Dummy::one()),
          key(nullptr),
          index(nullptr) {
}

EvalPlan::EvalPlan(ColumnNames *projection, EvalPlan *relation)
        : type(Project),
          relation(relation),
          projection(projection),
          select_conjunction(nullptr),
          table(Dummy::one()),
          key(nullptr),
          index(nullptr) {
}

EvalPlan::EvalPlan(ValueDict* conjunction, EvalPlan *relation)
        : type(Select),
          relation(relation),
          projection(nullptr),
          select_conjunction(conjunction),
          table(Dummy::one()),
          key(nullptr),
          index(nullptr) {
}

EvalPlan::EvalPlan(DbRelation &table)
        : type(TableScan),
          relation(nullptr),
          projection(nullptr),
          select_conjunction(nullptr),
          table(table),
          key(nullptr),
          index(nullptr) {
}

EvalPlan::EvalPlan(ValueDict *key, DbIndex *index)
        : type(IndexLookup),
          relation(nullptr),
          projection(nullptr),
          select_conjunction(nullptr),
          table(Dummy::one()),
          key(key),
          index(index) {
}

EvalPlan::EvalPlan(const EvalPlan *other)
        : type(other->type), table(other->table) {
    if (other->relation != nullptr)
        relation = new EvalPlan(other->relation);
    else
        relation = nullptr;

    if (other->projection != nullptr)
        projection = new ColumnNames(*other->projection);
    else
        projection = nullptr;

    if (other->select_conjunction != nullptr)
        select_conjunction = new ValueDict(*other->select_conjunction);
    else
        select_conjunction = nullptr;

    if (other->key != nullptr)
        key = new ValueDict(*other->key);
    else
        key = nullptr;

    if (other->index != nullptr)
        index = other->index;
    else
        index = nullptr;
}

EvalPlan::~EvalPlan() {
    delete relation;
    delete projection;
    delete select_conjunction;
    delete key;
}


EvalPlan *EvalPlan::optimize() {
    switch(this->type) {
        case ProjectAll:
            return new EvalPlan(EvalPlan::ProjectAll, this->relation->optimize());

        case Project:
            return new EvalPlan(new ColumnNames(*this->projection), this->relation->optimize());

        case Select:
            // look for index lookup opportunity
            if (this->relation->type == TableScan) {
                for (auto const& index_name: SQLExec::indices->get_index_names(this->relation->table.get_table_name())) {
                    DbIndex &index = SQLExec::indices->get_index(this->relation->table, index_name);
                    // if this index starts with a value that's in our where clause, then figure it's best to use index
                    if (this->select_conjunction->find(index.get_key_columns()[0]) != this->select_conjunction->end()) {
                        ValueDict *key = new ValueDict();
                        for (Identifier const& cn: index.get_key_columns()) {
                            if (this->select_conjunction->find(cn) != this->select_conjunction->end())
                                (*key)[cn] = (*this->select_conjunction)[cn];
                            return new EvalPlan(key, &index);
                            // FIXME: not quite done since we have to account for any non-key values in the select_conjunction
                        }
                    }
                }
            }
            return new EvalPlan(new ValueDict(*this->select_conjunction), this->relation->optimize());

        case TableScan:
        case IndexLookup:
        default:
            break;
    }
    return new EvalPlan(this);  // For now, we don't know how to do anything better
}

ValueDicts *EvalPlan::evaluate() {
    ValueDicts *ret = nullptr;
    if (this->type != ProjectAll && this->type != Project)
        throw DbRelationError("Invalid evaluation plan--not ending with a projection");

    EvalPipeline pipeline = this->relation->pipeline();
    DbRelation *temp_table = pipeline.first;
    Handles *handles = pipeline.second;
    if (this->type == ProjectAll)
        ret = temp_table->project(handles);
    else if (this->type == Project)
        ret = temp_table->project(handles, this->projection);
    delete handles;
    return ret;
}

EvalPipeline EvalPlan::pipeline() {
    // base cases
    if (this->type == TableScan)
        return EvalPipeline(&this->table, this->table.select());
    if (this->type == Select && this->relation->type == TableScan)
        return EvalPipeline(&this->relation->table, this->relation->table.select(this->select_conjunction));
    if (this->type == IndexLookup)
        return EvalPipeline(&this->index->get_relation(), this->index->lookup(this->key));

    // recursive case
    if (this->type == Select) {
        EvalPipeline pipeline = this->relation->pipeline();
        DbRelation *temp_table = pipeline.first;
        Handles *handles = pipeline.second;
        EvalPipeline ret(temp_table, temp_table->select(handles, this->select_conjunction));
        delete handles;
        return ret;
    }

    throw DbRelationError("Not implemented: pipeline other than Select or TableScan");
}
