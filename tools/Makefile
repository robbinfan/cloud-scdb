# set CPUS for Linux or FreeBSD
PLATFORM := $(shell uname)
CPUS := $(strip $(if $(shell echo $(PLATFORM)|grep Linux),\
	$(shell cat /proc/cpuinfo|grep -c processor),\
	$(shell sysctl -a | egrep -i 'hw.ncpu' | cut -d: -f2)))

CXX := g++

CXXFLAGS := -DNDEBUG -O9 -g3 -fno-strict-aliasing -Wall -Werror -fPIC \
	-std=c++0x \
	-I . \
	-I ../include \
	-I ../src

LDFLAGS := -L/usr/local/lib -L../src

RTFLAGS := -Wl,-rpath=../src

LIBS := -lscdb -lglog

SRC := $(wildcard *.cc) \
	   $(wildcard utils/*.cc)
OBJ := $(patsubst %.cc, %.o, $(SRC))
DEP := $(patsubst %.o, %.d, $(OBJ))

TARGET := set-builder map-builder

all:
	$(MAKE) target

set-builder: build-set.o cmdopt.o
	$(CXX) $^ -o $@ $(RTFLAGS) $(LDFLAGS) $(LIBS)

map-builder: build-map.o cmdopt.o
	$(CXX) $^ -o $@ $(RTFLAGS) $(LDFLAGS) $(LIBS)

target: $(TARGET)

%.o : %.cc
	$(CXX) -c $(CXXFLAGS) $< -o $@

%.d : %.cc
	@$(CXX) -MM $< $(CXXFLAGS) | sed 's/$(notdir $*)\.o/$(subst /,\/,$*).o $(subst /,\/,$*).d/g' > $@

clean:
	-rm -rf $(OBJ) $(TARGET)

test: all
	$(MAKE) test -C test

.PHONY: all target clean test
