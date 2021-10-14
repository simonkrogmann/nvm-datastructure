#!/bin/bash

g++ -O3 -march=native -std=c++11 -DNDEBUG -m64 -D_REENTRANT -fno-strict-aliasing -DINTEL -Wno-unused-value -Wno-format  -o ./main  ./main.cpp -lprofiler
g++ -g -std=c++11 -D_REENTRANT -fno-strict-aliasing -DINTEL -Wno-unused-value -Wno-format  -o ./main_debug  ./main.cpp -lprofiler -g -DUSE_PMDK -lpmemobj
