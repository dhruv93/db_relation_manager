CCFLAGS     = -std=c++11 -std=c++0x -Wall -Wno-c++11-compat -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -c
BDB         = /usr/local/db6
PARSER      = $(HOME)/repos/sql-parser
LIBS        = -ldb_cxx -lsqlparser
OBJS        = sql4300.o heap_storage.o ParseTreeToString.o SQLExec.o schema_tables.o storage_engine.o


%.o: %.cpp
	g++ -I$(BDB)/include -I$(PARSER)/src $(CCFLAGS) -o "$@" "$<"

sql4300: $(OBJS)
	g++ -L$(BDB)/lib -L$(PARSER) -o $@ $(OBJS) $(LIBS)
	
all: sql4300
	echo "success"

clean:
	rm sql4300 $(OBJS)
	
