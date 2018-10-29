CC = gcc
CFLAGS  = -g3 -Wall
LIBS += -lm


rhj:  main.o results.o hash1.o hash2.o files.o testing.o
	$(CC) $(CFLAGS) -o rhj main.o results.o hash1.o hash2.o files.o testing.o $(LIBS)


hash1.o:  hash1.c hash1.h 
	$(CC) $(CFLAGS) -c hash1.c

hash2.o:  hash2.c hash2.h 
	$(CC) $(CFLAGS) -c hash2.c

files.o: files.c files.h
	$(CC) $(CFLAGS) -c files.c

results.o: results.c results.h 
	$(CC) $(CFLAGS) -c results.c

testing.o: testing.c testing.h 
	$(CC) $(CFLAGS) -c testing.c


clean: 
	$(RM) rhg *.o * ~
	$(RM) hash1 *.o * ~
	$(RM) hash2 *.o * ~
	$(RM) files *.o * ~
	$(RM) testing *.o * ~
