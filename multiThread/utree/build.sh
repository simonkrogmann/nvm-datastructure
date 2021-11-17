#!/bin/bash

g++ -g -std=c++17 -DKEYSIZE=21 -m64 -D_REENTRANT -fno-strict-aliasing -I./atomic_ops -DINTEL -Wno-unused-value -Wno-format -o ./main-gu-zipfian main-gu-zipfian.c -lpmemobj -lpmem -lpthread \
    -Wall -Wextra -Wpedantic -Wfloat-equal -Wno-sign-compare -fsanitize=undefined -fsanitize=address -fno-sanitize-recover=all
