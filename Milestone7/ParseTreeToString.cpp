//
// Created by Kevin Lundeen on 4/6/17.
//

#include "ParseTreeToString.h"

const std::vector<std::string> ParseTreeToString::reserved_words = {
"COLUMNS", "SHOW", "TABLES",
"ADD","ALL","ALLOCATE","ALTER","AND","ANY","ARE","ARRAY","AS","ASENSITIVE","ASYMMETRIC","AT",
                  "ATOMIC","AUTHORIZATION","BEGIN","BETWEEN","BIGINT","BINARY","BLOB","BOOLEAN","BOTH","BY","CALL",
                  "CALLED","CASCADED","CASE","CAST","CHAR","CHARACTER","CHECK","CLOB","CLOSE","COLLATE","COLUMN",
                  "COMMIT","CONNECT","CONSTRAINT","CONTINUE","CORRESPONDING","CREATE","CROSS","CUBE","CURRENT",
                  "CURRENT_DATE","CURRENT_DEFAULT_TRANSFORM_GROUP","CURRENT_PATH","CURRENT_ROLE","CURRENT_TIME",
                  "CURRENT_TIMESTAMP","CURRENT_TRANSFORM_GROUP_FOR_TYPE","CURRENT_USER","CURSOR","CYCLE","DATE",
                  "DAY","DEALLOCATE","DEC","DECIMAL","DECLARE","DEFAULT","DELETE","DEREF","DESCRIBE","DETERMINISTIC",
                  "DISCONNECT","DISTINCT","DOUBLE","DROP","DYNAMIC","EACH","ELEMENT","ELSE","END","END-EXEC","ESCAPE",
                  "EXCEPT","EXEC","EXECUTE","EXISTS","EXTERNAL","FALSE","FETCH","FILTER","FLOAT","FOR","FOREIGN",
                  "FREE","FROM","FULL","FUNCTION","GET","GLOBAL","GRANT","GROUP","GROUPING","HAVING","HOLD","HOUR",
                  "IDENTITY","IMMEDIATE","IN","INDICATOR","INNER","INOUT","INPUT","INSENSITIVE","INSERT","INT",
                  "INTEGER","INTERSECT","INTERVAL","INTO","IS","ISOLATION","JOIN","LANGUAGE","LARGE","LATERAL",
                  "LEADING","LEFT","LIKE","LOCAL","LOCALTIME","LOCALTIMESTAMP","MATCH","MEMBER","MERGE","METHOD",
                  "MINUTE","MODIFIES","MODULE","MONTH","MULTISET","NATIONAL","NATURAL","NCHAR","NCLOB","NEW","NO",
                  "NONE","NOT","NULL","NUMERIC","OF","OLD","ON","ONLY","OPEN","OR","ORDER","OUT","OUTER","OUTPUT",
                  "OVER","OVERLAPS","PARAMETER","PARTITION","PRECISION","PREPARE","PRIMARY","PROCEDURE","RANGE",
                  "READS","REAL","RECURSIVE","REF","REFERENCES","REFERENCING","REGR_AVGX","REGR_AVGY","REGR_COUNT",
                  "REGR_INTERCEPT","REGR_R2","REGR_SLOPE","REGR_SXX","REGR_SXY","REGR_SYY","RELEASE","RESULT","RETURN",
                  "RETURNS","REVOKE","RIGHT","ROLLBACK","ROLLUP","ROW","ROWS","SAVEPOINT","SCROLL","SEARCH","SECOND",
                  "SELECT","SENSITIVE","SESSION_USER","SET","SIMILAR","SMALLINT","SOME","SPECIFIC","SPECIFICTYPE",
                  "SQL","SQLEXCEPTION","SQLSTATE","SQLWARNING","START","STATIC","SUBMULTISET","SYMMETRIC","SYSTEM",
                  "SYSTEM_USER","TABLE","THEN","TIME","TIMESTAMP","TIMEZONE_HOUR","TIMEZONE_MINUTE","TO","TRAILING",
                  "TRANSLATION","TREAT","TRIGGER","TRUE","UESCAPE","UNION","UNIQUE","UNKNOWN","UNNEST","UPDATE",
                  "UPPER","USER","USING","VALUE","VALUES","VAR_POP","VAR_SAMP","VARCHAR","VARYING","WHEN","WHENEVER",
                  "WHERE","WIDTH_BUCKET","WINDOW","WITH","WITHIN","WITHOUT","YEAR"};

bool ParseTreeToString::is_reserved_word(std::string candidate) {
    for(auto const& word: reserved_words)
        if (candidate == word)
            return true;
    return false;
}

std::string ParseTreeToString::operator_expression(const hsql::Expr *expr) {
    if (expr == NULL)
        return "null";

    std::string ret;
    if (expr->opType == hsql::Expr::NOT)
        ret += "NOT ";
    ret += expression(expr->expr) + " ";
    switch (expr->opType) {
        case hsql::Expr::SIMPLE_OP:
            ret += expr->opChar;
            break;
        case hsql::Expr::AND:
            ret += "AND";
            break;
        case hsql::Expr::OR:
            ret += "OR";
            break;
        case hsql::Expr::NONE:break;
        case hsql::Expr::BETWEEN:break;
        case hsql::Expr::CASE:break;
        case hsql::Expr::NOT_EQUALS:break;
        case hsql::Expr::LESS_EQ:break;
        case hsql::Expr::GREATER_EQ:break;
        case hsql::Expr::LIKE:break;
        case hsql::Expr::NOT_LIKE:break;
        case hsql::Expr::IN:break;
        case hsql::Expr::NOT:break;
        case hsql::Expr::UMINUS:break;
        case hsql::Expr::ISNULL:break;
        case hsql::Expr::EXISTS:break;
    }
    if (expr->expr2 != NULL)
        ret += " " + expression(expr->expr2);
    return ret;
}

std::string ParseTreeToString::expression(const hsql::Expr *expr) {
    std::string ret;
    switch (expr->type) {
        case hsql::kExprStar:
            ret += "*";
            break;
        case hsql::kExprColumnRef:
            if (expr->table != NULL)
                ret += std::string(expr->table) + ".";
            ret += expr->name;
            break;
        case hsql::kExprLiteralString:
            ret += std::string("\"") + expr->name + "\"";
            break;
        case hsql::kExprLiteralFloat:
            ret += std::to_string(expr->fval);
            break;
        case hsql::kExprLiteralInt:
            ret += std::to_string(expr->ival);
            break;
        case hsql::kExprFunctionRef:
            ret += std::string(expr->name) + "?" + expr->expr->name;
            break;
        case hsql::kExprOperator:
            ret += operator_expression(expr);
            break;
        default:
            ret += "???";
            break;
    }
    if (expr->alias != NULL)
        ret += std::string(" AS ") + expr->alias;
    return ret;
}

std::string ParseTreeToString::table_ref(const hsql::TableRef *table) {
    std::string ret;
    switch (table->type) {
        case hsql::kTableSelect:
            break;
        case hsql::kTableName:
            ret += table->name;
            if (table->alias != NULL)
                ret += std::string(" AS ") + table->alias;
            break;
        case hsql::kTableJoin:
            ret += table_ref(table->join->left);
            switch (table->join->type) {
                case hsql::kJoinCross:
                case hsql::kJoinInner:
                    ret += " JOIN ";
                    break;
                case hsql::kJoinOuter:
                case hsql::kJoinLeftOuter:
                case hsql::kJoinLeft:
                    ret += " LEFT JOIN ";
                    break;
                case hsql::kJoinRightOuter:
                case hsql::kJoinRight:
                    ret += " RIGHT JOIN ";
                    break;
                case hsql::kJoinNatural:
                    ret += " NATURAL JOIN ";
                    break;
            }
            ret += table_ref(table->join->right);
            if (table->join->condition != NULL)
                ret += " ON " + expression(table->join->condition);
            break;
        case hsql::kTableCrossProduct:
            bool doComma = false;
            for (hsql::TableRef *tbl : *table->list) {
                if (doComma)
                    ret += ", ";
                ret += table_ref(tbl);
                doComma = true;
            }
            break;
    }
    return ret;
}

std::string ParseTreeToString::column_definition(const hsql::ColumnDefinition *col) {
    std::string ret;
    if (col->definitionType == hsql::ColumnDefinition::kColumn) {
        ret += col->name;
        switch (col->type) {
            case hsql::ColumnDefinition::DOUBLE:
                ret += " DOUBLE";
                break;
            case hsql::ColumnDefinition::INT:
                ret += " INT";
                break;
            case hsql::ColumnDefinition::TEXT:
                ret += " TEXT";
                break;
            default:
                ret += " ...";
                break;
        }
    } else {
        ret += "PRIMARY KEY (";
        bool doComma = false;
        for (auto const& pkcol : *col->primaryKeyColumns) {
            if (doComma)
                ret += ", ";
            ret += pkcol;
            doComma = true;
        }
        ret += ")";
    }
    return ret;
}

std::string ParseTreeToString::select(const hsql::SelectStatement *stmt) {
    std::string ret("SELECT ");
    bool doComma = false;
    for (hsql::Expr *expr : *stmt->selectList) {
        if (doComma)
            ret += ", ";
        ret += expression(expr);
        doComma = true;
    }
    ret += " FROM " + table_ref(stmt->fromTable);
    if (stmt->whereClause != NULL)
        ret += " WHERE " + expression(stmt->whereClause);
    return ret;
}

std::string ParseTreeToString::insert(const hsql::InsertStatement *stmt) {
    std::string ret("INSERT INTO ");
    ret += stmt->tableName;
    if (stmt->type == hsql::InsertStatement::kInsertSelect)
        return ret + "SELECT ...";

    bool doComma = false;
    if (stmt->columns != NULL) {
        ret += " (";
        for (auto const &column: *stmt->columns) {
            if (doComma)
                ret += ", ";
            ret += column;
            doComma = true;
        }
        ret += ")";
    }
    ret += " VALUES (";
    doComma = false;
    for (hsql::Expr *expr : *stmt->values) {
        if (doComma)
            ret += ", ";
        ret += expression(expr);
        doComma = true;
    }
    ret += ")";
    return ret;
}

std::string ParseTreeToString::create(const hsql::CreateStatement *stmt) {
    std::string ret("CREATE ");
    if (stmt->type == hsql::CreateStatement::kTable) {
        ret += "TABLE ";
        if (stmt->ifNotExists)
            ret += "IF NOT EXISTS ";
        ret += std::string(stmt->tableName) + " (";
        bool doComma = false;
        for (hsql::ColumnDefinition *col : *stmt->columns) {
            if (doComma)
                ret += ", ";
            ret += column_definition(col);
            doComma = true;
        }
        ret += ")";
    } else if (stmt->type == hsql::CreateStatement::kIndex) {
        ret += "INDEX ";
        ret += std::string(stmt->indexName) + " ON ";
        ret += std::string(stmt->tableName) + " USING " + stmt->indexType + " (";
        bool doComma = false;
        for (auto const& col : *stmt->indexColumns) {
            if (doComma)
                ret += ", ";
            ret += std::string(col);
            doComma = true;
        }
        ret += ")";
    } else {
        ret += "...";
    }
    return ret;
}

std::string ParseTreeToString::drop(const hsql::DropStatement *stmt) {
    std::string  ret("DROP ");
    switch(stmt->type) {
        case hsql::DropStatement::kTable:
            ret += "TABLE ";
            break;
        case hsql::DropStatement::kIndex:
            ret += std::string("INDEX ") + stmt->indexName + " FROM ";
            break;
        default:
            ret += "? ";
    }
    ret += stmt->name;
    return ret;
}

std::string ParseTreeToString::show(const hsql::ShowStatement *stmt) {
    std::string ret("SHOW ");
    switch (stmt->type) {
        case hsql::ShowStatement::kTables:
            ret += "TABLES";
            break;
        case hsql::ShowStatement::kColumns:
            ret += std::string("COLUMNS FROM ") + stmt->tableName;
            break;
        case hsql::ShowStatement::kIndex:
            ret += std::string("INDEX FROM ") + stmt->tableName;
            break;
        default:
            ret += "?what?";
            break;
    }
    return ret;
}

std::string ParseTreeToString::del(const hsql::DeleteStatement *stmt) {
    std::string ret("DELETE FROM ");
    ret += stmt->tableName;
    if (stmt->expr != NULL) {
        ret += " WHERE ";
        ret += expression(stmt->expr);
    }
    return ret;
}

std::string ParseTreeToString::statement(const hsql::SQLStatement *stmt) {
    switch (stmt->type()) {
        case hsql::kStmtSelect:
            return select((const hsql::SelectStatement *) stmt);
        case hsql::kStmtInsert:
            return insert((const hsql::InsertStatement *) stmt);
        case hsql::kStmtDelete:
            return del((const hsql::DeleteStatement *) stmt);
        case hsql::kStmtCreate:
            return create((const hsql::CreateStatement *) stmt);
        case hsql::kStmtDrop:
            return drop((const hsql::DropStatement *) stmt);
        case hsql::kStmtShow:
            return show((const hsql::ShowStatement *) stmt);

        case hsql::kStmtError:
        case hsql::kStmtImport:
        case hsql::kStmtUpdate:
        case hsql::kStmtPrepare:
        case hsql::kStmtExecute:
        case hsql::kStmtExport:
        case hsql::kStmtRename:
        case hsql::kStmtAlter:
        default:
            return "Not implemented";
    }
}
