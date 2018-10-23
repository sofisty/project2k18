CC = gcc
CFLAGS  = -g3 -Wall
LIBS += -lm


hash:  hash1.o hash2.o files.o
	$(CC) $(CFLAGS) -o hash hash1.o hash2.o files.o $(LIBS)


hash1.o:  hash1.c hash1.h 
	$(CC) $(CFLAGS) -c hash1.c

hash2.o:  hash2.c hash2.h 
	$(CC) $(CFLAGS) -c hash2.c

files.o: files.c files.h
	$(CC) $(CFLAGS) -c files.c



clean: 
	$(RM) hash1 *.o * ~
	$(RM) hash2 *.o * ~
