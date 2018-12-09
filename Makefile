CC = gcc
CFLAGS  = -g3 -Wall
LIBS += -lm


rhj:  main.o results.o hash1.o hash2.o files.o infomap.o interm.o query.o join.o
	$(CC) $(CFLAGS) -o rhj main.o results.o hash1.o hash2.o files.o infomap.o interm.o query.o join.o $(LIBS)

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

interm.o: interm.c interm.h 
	$(CC) $(CFLAGS) -c interm.c

infomap.o: infomap.c infomap.h 
	$(CC) $(CFLAGS) -c infomap.c

join.o: join.c join.h 
	$(CC) $(CFLAGS) -c join.c


clean: 
	-rm -f *.o 
	-rm -f rhj
