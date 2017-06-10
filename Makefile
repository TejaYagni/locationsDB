CC = g++
CCFLAGS = -Wall -Werror

all: ./dist ./file1 ./file2

dist: dist.o
	$(CC) -o dist dist.o -lpq
dist.o: dist.cc
	$(CC) -c $(CCFLAGS) dist.cc
file1: file1.o
	$(CC) -o file1 file1.o -lpq
file1.o: file1.cc
	$(CC) -c $(CCFLAGS) file1.cc
file2: file2.o
	$(CC) -o $ file2 file2.o -lpq
file2.o: file2.cc
	$(CC) -c $(CCFLAGS) file2.cc

clean:
	-rm *.o dist file1 file2 *.cc~
