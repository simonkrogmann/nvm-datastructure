#!/bin/bash

g++ -g -std=c++14 -m64 -D_REENTRANT -fno-strict-aliasing -I./atomic_ops -DINTEL -Wno-unused-value -Wno-format  -o ./main-gu-zipfian main-gu-zipfian.c -m64 -lpmemobj -lpmem -lpthread
