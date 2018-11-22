CC = gcc
CFLAGS  = -g3 -Wall
LIBS += -lm


rhj:  main.o results.o hash1.o hash2.o files.o testing.o testfiles.o query.o join.o
	$(CC) $(CFLAGS) -o rhj main.o results.o hash1.o hash2.o files.o testing.o testfiles.o query.o join.o $(LIBS)

query.o:  query.c query.h
	$(CC) $(CFLAGS) -c query.c

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

testfiles.o: testfiles.c testfiles.h 
	$(CC) $(CFLAGS) -c testfiles.c

join.o: join.c join.h 
	$(CC) $(CFLAGS) -c join.c


clean: 
	$(RM) rhj *.o * ~
