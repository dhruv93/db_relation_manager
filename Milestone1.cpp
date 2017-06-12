#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sstream>
#include "SQLParser.h"
#include "sql/SelectStatement.h"
#include "sql/CreateStatement.h"
#include "sql/Table.h"
#include "sql/Expr.h"
#include "db_cxx.h"

using namespace hsql;
using namespace std;

/*
* ======================================================================
* Function Declarations
* ======================================================================
*/
std::string printExpression(Expr* expr);
std::string printSelectStatement(const SelectStatement* stmt);
std::string printTableInfo(TableRef* table);


/*
 * ======================================================================
 * Print table information
 * ======================================================================
 */
std::string printTableInfo(TableRef* table) {

	std::string tableStr = "";

	std::string leftTableName;
	std::string rightTableName;
	std::string joinCondition;
	std::string joinType;

	switch (table->type) {

		case kTableName:
			tableStr += table->name;
			break;

		case kTableSelect:
			tableStr += printSelectStatement(table->select);
			break;

		case kTableJoin: {

			leftTableName = printTableInfo(table->join->left);
			rightTableName = printTableInfo(table->join->right);
			joinCondition = printExpression(table->join->condition);

			switch (table->join->type) {

				case kJoinInner: joinType = "INNER JOIN"; break;
				case kJoinOuter: joinType = "OUTER JOIN"; break;
				case kJoinLeft: joinType = "LEFT JOIN"; break;
				case kJoinRight: joinType = "RIGHT JOIN"; break;
				case kJoinLeftOuter: joinType = "LEFT OUTER JOIN"; break;
				case kJoinRightOuter: joinType = "RIGHT OUTER JOIN"; break;
				case kJoinCross: joinType = "CROSS JOIN"; break;
				case kJoinNatural: joinType = "NATURAL JOIN"; break;
			}

			tableStr += leftTableName
				+ " " + joinType
				+ " " + rightTableName
				+ " ON " + joinCondition;

			joinType.clear();
			leftTableName.clear();
			rightTableName.clear();
			joinCondition.clear();
			break;
		}

		case kTableCrossProduct:
			for (TableRef* temp : *table->list)
				tableStr += printTableInfo(temp);
			break;
	}

	if (table->alias != NULL) 
		tableStr += " AS " + std::string(table->alias) + " ";
	
	return tableStr;
}


/*
* ======================================================================
* Print Operator information
* ======================================================================
*/
std::string printOperatorExpression(Expr* expr) {

	std::string opStr = "";
	std::ostringstream oss;

	if (expr == NULL) {
		opStr == "";
	}

	//print LHS
	opStr.append(printExpression(expr->expr));

	//print operator
	switch (expr->opType) {

		case Expr::SIMPLE_OP:
			opStr += expr->opChar;
			break;
		case Expr::AND:
			opStr.append("AND ");
			break;
		case Expr::OR:
			opStr.append("OR ");
			break;
		case Expr::NOT:
			opStr.append("NOT ");
			break;
		default:
			oss << expr->opType;
			opStr.append(oss.str());
			cout <<"Default "<< oss.str() << endl;
			oss.clear();
			break;
	}
	
	//Print RHS
	if (expr->expr2 != NULL) 
		opStr.append(printExpression(expr->expr2));

	return opStr;
}


/*
* ======================================================================
* Print Operator information
* ======================================================================
*/
std::string printExpression(Expr* expr) {

	std:string exprString = "";
	std::ostringstream oss;

	switch (expr->type) {

		case kExprStar:
			exprString.append("* ");
			break;

		case kExprColumnRef: {
			if (expr->table != NULL) {		//check if we need to insert table name
				exprString.append(expr->table);
				exprString.append(".");
			}
			exprString.append(expr->name);
			break;
		}

		case kExprLiteralFloat: 
			oss << expr->fval;
			exprString.append(oss.str());
			oss.clear();
			break;
		case kExprLiteralInt:
			oss << expr->ival;
			exprString.append(oss.str());
			oss.clear();
			break;
		case kExprLiteralString:
			exprString.append(expr->name);
			break;
		case kExprFunctionRef:
			exprString.append(expr->name);
			exprString.append(expr->expr->name);
			break;
		case kExprOperator:
			exprString.append(printOperatorExpression(expr));
			break;
		default:
			fprintf(stderr, "Unrecognized expression type %d\n", expr->type);
			return "ERROR";
	}
	if (expr->alias != NULL) {
		exprString.append("as ");
		exprString.append(expr->alias);
	}
	return exprString;
}


/*
* ======================================================================
* Print Select statement
* ======================================================================
*/
string printSelectStatement(const SelectStatement* stmt) {

	std::string output = "";
	output.append("SELECT ");

	//print fields
	for (Expr* expr : *stmt->selectList) {
		output.append(printExpression(expr));
		output.append(", ");
	}
	//cleanup last occurence of ','
	string::size_type pos = output.find_last_of(",");
	if (pos != string::npos)
	{
		output.erase(pos);
	}

	//print "FROM" clause
	output.append(" FROM ");
	output.append(printTableInfo(stmt->fromTable));
	output.append(" ");
	
	//check "WHERE" clause
	if (stmt->whereClause != NULL) {
		output.append("WHERE ");
		output.append(printExpression(stmt->whereClause));
		output.append(" ");
	}

	//Check union
	if (stmt->unionSelect != NULL) {
		output.append("\n\tUNION\n");
		output.append("\t " + printSelectStatement(stmt->unionSelect));
		output.append(" ");
	}

	//Check Order BY
	if (stmt->order != NULL) {
		output.append("ORDER BY ");
		output.append(
			printExpression(stmt->order->at(0)->expr)
		);
		if (stmt->order->at(0)->type == kOrderAsc)
			output.append(" ascending ");
		else
			output.append(" descending ");
	}

	return output;
}


/*
* ======================================================================
* Print Create statement
* ======================================================================
*/
std::string printCreateStatement(const CreateStatement* stmt2) {
	
	std::string output;
	output = "CREATE TABLE " + std::string(stmt2->tableName);
	output.append(" (");

	//iterate over columns
	std::vector<ColumnDefinition*>* cols = stmt2->columns;
	std::vector<ColumnDefinition*>::iterator iter;

	for (iter = cols->begin(); iter != cols->end(); iter++) {
		output += std::string((*iter)->name) + " ";
		switch ((*iter)->type) {
			case 0: output += "unknown, "; break;
			case 1: output += "text, "; break;
			case 2: output += "int, "; break;
			case 3: output += "double, "; break;
		}
	}
	//cleanup last occurence of ','
	string::size_type pos = output.find_last_of(",");
	if (pos != string::npos)
	{
		output.erase(pos);
	}
	output.append(" )");
	return output;
}


/*
* ======================================================================
* Generic Print statement
* ======================================================================
*/
void printStatement(const SQLStatement* stmt, StatementType stmtType) {

	switch (stmtType) {
		case kStmtSelect: {
			cout<<printSelectStatement((const SelectStatement*) stmt)<<endl;
			break;
		}
		case kStmtCreate: {
			cout << printCreateStatement((const CreateStatement*)stmt) << endl;
			break;
		}
		default: {
			cout << "Not Supported" << endl;
			break;
		}
	}
}


/*
* ======================================================================
* Execute Method
* ======================================================================
*/
void execute(const SQLStatement* stmt) {

	printStatement(stmt, stmt->type());
}


/*
* ======================================================================
* Main
* ======================================================================
*/
int main()
{
	const char *HOME = "cpsc4300/data";
	
	const char *EXAMPLE = "temp.db";
	const unsigned int BLOCK_SZ = 4096;
	const char *home = std::getenv("HOME");

	std::string envdir = std::string(home) + "/" + HOME;

	DbEnv env(0U);
	env.set_message_stream(&std::cout);
	env.set_error_stream(&std::cerr);
	env.open(envdir.c_str(), DB_CREATE | DB_INIT_MPOOL, 0);

	cout << "Running with database environment at " << envdir.c_str() << endl;

	std::string query;
	cout << "Type 'quit' to quit."<<endl;

	while (true) {

		query.clear();
		cout << "\nSQL> ";
		getline(cin, query);

		if (query == "quit") {
			return 0;
		}

		hsql::SQLParserResult* result = hsql::SQLParser::parseSQLString(query);

		if (result->isValid()) {
			for (int i = 0; i < result->size(); i++) {
				execute(result->getStatement(i));
			}		
		}
		else {
			cout << "Invalid query : " << query;
		}
		query.clear();
		result = NULL;
		cin.clear();
		fflush(stdin);
	}
	return 0;
}



