
CC = g++ -O2 -Wno-deprecated

tag = -i

ifdef linux
tag = -n
endif
gtest.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o gtest.o
	$(CC) -o gtest.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o gtest.o -lfl -lpthread -lgtest

gtest2.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o HeapDBFile.o SortedDBFile.o DBFile.o Pipe.o y.tab.o lex.yy.o gtest2.o
	$(CC) -o gtest2.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o HeapDBFile.o SortedDBFile.o DBFile.o Pipe.o y.tab.o lex.yy.o gtest2.o -lfl -lpthread -lgtest


test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o HeapDBFile.o SortedDBFile.o DBFile.o  Pipe.o y.tab.o lex.yy.o test.o
	$(CC) -o test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o HeapDBFile.o SortedDBFile.o DBFile.o Pipe.o y.tab.o lex.yy.o test.o -lfl -lpthread

test2.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o HeapDBFile.o SortedDBFile.o DBFile.o  Pipe.o y.tab.o lex.yy.o test2.o
	$(CC) -o test2.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o HeapDBFile.o SortedDBFile.o  DBFile.o Pipe.o y.tab.o lex.yy.o test2.o -lfl -lpthread


a1test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o y.tab.o lex.yy.o a1-test.o
	$(CC) -o a1test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o y.tab.o lex.yy.o a1-test.o -lfl

gtest2.o: gtest2.cc
	$(CC) -g -c gtest2.cc

gtest.o: gtest.cc
	$(CC) -g -c gtest.cc
		
test.o: test.cc
	$(CC) -g -c test.cc

test2.o: test2.cc
	$(CC) -g -c test2.cc

a1-test.o: a1-test.cc
	$(CC) -g -c a1-test.cc

Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc
	
ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc
	
Pipe.o: Pipe.cc
	$(CC) -g -c Pipe.cc

BigQ.o: BigQ.cc
	$(CC) -g -c BigQ.cc
	
HeapDBFile.o : HeapDBFile.cc
	$(CC) -g -c HeapDBFile.cc  

SortedDBFile.o : SortedDBFile.cc
	$(CC) -g -c  SortedDBFile.cc

DBFile.o: DBFile.cc
	$(CC) -g -c DBFile.cc

GenericDBFile.o : GenericDBFile.h
	g++ -c  GenericDBFile.h

File.o: File.cc
	$(CC) -g -c File.cc

Record.o: Record.cc
	$(CC) -g -c Record.cc

Schema.o: Schema.cc
	$(CC) -g -c Schema.cc
	
y.tab.o: Parser.y
	yacc -d Parser.y
	sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c y.tab.c

lex.yy.o: Lexer.l
	lex  Lexer.l
	gcc  -c lex.yy.c

clean: 
	rm -f *.o
	rm -f *.out
	rm -f y.tab.c
	rm -f lex.yy.c
	rm -f y.tab.h
