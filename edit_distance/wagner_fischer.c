/*
This file is part of Liquid Fortress Distributed Election

Copyright (c) 2018. Richard Scott McNew. All rights reserved.

Developed by: Richard Scott McNew

Permission is hereby granted, free of charge, to any person obtaining a copy of this software 
and associated documentation files (the "Software"), to deal with the Software without restriction, 
including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, 
and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, 
subject to the following conditions:

Redistributions of source code must retain the above copyright notice, this list of conditions and the 
following disclaimers.

Redistributions in binary form must reproduce the above copyright notice, this list of conditions and 
the following disclaimers in the documentation and/or other materials provided with the distribution.

Neither the names of Richard Scott McNew, Liquid Fortress, nor the names of contributors may be used to 
endorse or promote products derived from this Software without specific prior written permission.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT 
LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN 
NO EVENT SHALL THE CONTRIBUTORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, 
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE 
SOFTWARE OR THE USE OR OTHER DEALINGS WITH THE SOFTWARE. 
*/
#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

int min_of_2(int a, int b) {
    if (a < b) {
        return a;
    } else {
        return b;
    }
}

int min(int a, int b, int c) {
    int the_min = min_of_2(min_of_2(a, b), c);
    //printf("min(%d, %d, %d) = %d\n", a, b, c, the_min);
    return the_min;
}

/*
Calculate the edit distance between two strings using the Wagner-Fischer algorithm 
(https://en.wikipedia.org/wiki/Wagner%E2%80%93Fischer_algorithm).
*/
int calculate_edit_distance(char* s, char* t) {
    int result = -1;
	if ((s == NULL) || (t == NULL)) {
		return result;
	}
	/* get the string lengths and create a 2D matrix to hold the calculation */
	int s_side = strlen(s) + 1;
    //printf("s_side is %d\n", s_side);
	int t_side = strlen(t) + 1;
    //printf("t_side is %d\n", t_side);
	
	int (*d)[t_side] = calloc(1, sizeof(int[s_side][t_side])); 

    //printf("filling along\n");
	/* fill along each string */
	for (int i = 0; i < s_side; i++) {
		d[i][0] = i;
	}
	for (int j = 0; j < t_side; j++) {
		d[0][j] = j;
	}
    //printf("filling cell-by-cell\n");
	/* fill d using cell-by-cell comparison */
	for (int j = 1; j < t_side; j++) {
		for (int i = 1; i < s_side; i++) {
			if (s[i-1] == t[j-1]) {
				d[i][j] = d[i-1][j-1];
			} else {
                d[i][j] = min( (d[i-1][j] + 1), (d[i][j-1] + 1), (d[i-1][j-1] + 1) );
			}
		}
	}
	/* debug: print the matrix  */
/*
	for (int j = 0; j < t_side; j++) {
		for (int i = 0; i < s_side; i++) {
            printf("[%d]", d[i][j]);
		}
        printf("\n");
	}
    printf("\n");
*/	
	result = d[s_side-1][t_side-1];
    free(d);
    return result;
}

/* 
"string_pair_file" is the path and filename of a text file containing a string pair
The string_pair_file should have strings on the first two lines of the file.
*/
int calculate_edit_distance_from_string_pair_file(const char* string_pair_file) {
    int result = -1;
    char* s = NULL;
    char* t = NULL;
    // read in s and t from the string_pair_file
    FILE* file_handle = fopen(string_pair_file, "r");
    if (file_handle != NULL) {
        size_t line_size = 0;
        ssize_t count = getline(&s, &line_size, file_handle);
        //printf("getline count for s is %d\n", (int)count);
        if (count > 0) {
            if (s[count - 1] == '\n') {
                //printf("Chomping s\n");
                s[count - 1] = '\0';  // chomp!
                //printf("s = %s\n", s);
            }
        }
        count = getline(&t, &line_size, file_handle);
        //printf("getline count for t is %d\n", (int)count);
        if (count > 0) {
            if (t[count - 1] == '\n') {
                //printf("Chomping t\n");
                t[count - 1] = '\0';  // chomp!
                //printf("t = %s\n", t);
            }
        }
        // calculate the edit distance
        //printf("calculating edit distance\n");
        result = calculate_edit_distance(s, t);
        //printf("s = %s\nt = %s\nedit_distance = %d\n", s, t, result);
        if (s != NULL) {
            free(s);
        }
        if (t != NULL) {
            free(t);
        }
        fclose(file_handle);
    }
    return result;
} 

/*
#include <assert.h>
int unit_test() {
    char* s0 = "kitten";
    char* s1 = "sitting";
    char* s2 = "Saturday";
    char* s3 = "Sunday";
    int result = calculate_edit_distance(s0, s1);
    assert(result == 3);
    result = calculate_edit_distance(s2, s3);
    assert(result == 3);
}

int main(int argc, char** argv) {
    printf("string_pair_file is %s\n", argv[1]);
    return calculate_edit_distance_from_string_pair_file(argv[1]);
}
*/
