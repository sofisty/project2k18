CC = gcc
CFLAGS  = -g3 -Wall
LIBS += -lm

default: hash


hash:  hash1.o hash2.o
	$(CC) $(CFLAGS) -o hash hash1.o hash2.o $(LIBS)


hash1.o:  hash1.c hash1.h 
	$(CC) $(CFLAGS) -c hash1.c

hash2.o:  hash2.c hash2.h 
	$(CC) $(CFLAGS) -c hash2.c


clean: 
	$(RM) hash1 *.o * ~
	$(RM) hash2 *.o * ~
