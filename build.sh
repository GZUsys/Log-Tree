#!/bin/bash

g++ -O0 -g -std=c++11 -m64  -D_REENTRANT -fno-strict-aliasing -I./atomic_ops -DINTEL -Wno-unused-value -Wno-format  -o ./main main.c -lpmemobj -lpmem -lpthread -I/home/yzz/patch/pcm-master/src/daemon  -L/home/yzz/patch/pcm-master/build/lib -lpcm -Wint-to-pointer-cast -Wreturn-type 