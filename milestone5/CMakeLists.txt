cmake_minimum_required(VERSION 3.7)
project(sql4300)

set(CMAKE_CXX_STANDARD 11)

set(SOURCE_FILES
        heap_storage.cpp
        heap_storage.h
        sql4300.cpp
        storage_engine.h ParseTreeToString.cpp ParseTreeToString.h SQLExec.cpp SQLExec.h schema_tables.h schema_tables.cpp storage_engine.cpp EvalPlan.cpp EvalPlan.h)

include_directories(/usr/local/db6/include)
include_directories(~/sql-parser/src)

add_executable(sql4300 ${SOURCE_FILES})

find_library(BERKELEY_DB db_cxx HINTS "/usr/local/db6/lib")
target_link_libraries(sql4300 ${BERKELEY_DB})

find_library(SQL_PARSER SQLParser HINTS "~/sql-parser")
target_link_libraries(sql4300 ${SQL_PARSER})
