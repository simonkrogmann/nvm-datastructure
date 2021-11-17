#!/usr/bin/python3

import subprocess
import sys
import time


def compile(keysize):
    subprocess.run([
        "g++", "-std=c++17", "-m64", "-D_REENTRANT", "-fno-strict-aliasing",
        "-I./atomic_ops", "-DINTEL", "-Wno-unused-value", "-Wno-format",
        "-o", "./experiment.o", "main-gu-zipfian.c", "-lpmemobj", "-lpmem",
        # "-g", # debug
        "-Og", "-lpthread", f"-DKEYSIZE={keysize}"], check=True)


def run():
    try:
        output = subprocess.check_output(["./experiment.o"], stderr=subprocess.PIPE)
    except subprocess.CalledProcessError as error:
        print("Status : FAIL", error.returncode)
        print(f'stderr: {error.stderr.decode(sys.getfilesystemencoding())}')
        return -1
    return output.decode('utf-8')


def main():
    output = ""
    for keysize in range(1, 100):
        print(f"Running keysize {keysize}")
        compile(keysize)
        output += f"\nKeysize = {keysize}\n"
        res = run()
        if res == -1:
            output += "    Error!\n"
        else:
            output += run()
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    with open(f'keysize-experiment-{timestamp}.txt', 'w') as f:
        f.write(output)


if __name__ == '__main__':
    main()
