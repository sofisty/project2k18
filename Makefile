CXX = g++
CXXFLAGS += -std=c++11 -Wall -Wextra -g
LIBS += -lm -pthread


rhj:  main.o results.o hash1.o hash2.o infomap.o interm.o query.o join.o jobScheduler.o optim.o
	$(CXX) $(CXXFLAGS) -o rhj main.o results.o hash1.o hash2.o infomap.o interm.o query.o join.o jobScheduler.o optim.o $(LIBS)

query.o:  query.cpp query.h
	$(CXX) $(CXXFLAGS) -c query.cpp

hash1.o:  hash1.cpp hash1.h 
	$(CXX) $(CXXFLAGS) -c hash1.cpp

hash2.o:  hash2.cpp hash2.h 
	$(CXX) $(CXXFLAGS) -c hash2.cpp

results.o: results.cpp results.h 
	$(CXX) $(CXXFLAGS) -c results.cpp

interm.o: interm.cpp interm.h 
	$(CXX) $(CXXFLAGS) -c interm.cpp

infomap.o: infomap.cpp infomap.h 
	$(CXX) $(CXXFLAGS) -c infomap.cpp

join.o: join.cpp join.h 
	$(CXX) $(CXXFLAGS) -c join.cpp

jobScheduler.o: jobScheduler.cpp jobScheduler.h
	$(CXX) $(CXXFLAGS) -c jobScheduler.cpp

optim.o: optim.cpp optim.h
	$(CXX) $(CXXFLAGS) -c optim.cpp

clean: 
	-rm -f *.o 
	-rm -f rhj