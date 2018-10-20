CC = gcc
CFLAGS  = -g3 -Wall

default: hash


hash:  hash1.o 
	$(CC) $(CFLAGS) -o hash hash1.o 


hash1.o:  hash1.c hash1.h 
	$(CC) $(CFLAGS) -c hash1.c


clean: 
	$(RM) hash1 *.o *~ 