CC=g++
CFLAGS=-g -O3 --std=c++17

all:	generator

generator:	generator.cpp
	$(CC) $^ -o $@ $(CFLAGS)

clean:
	rm -rf generator
