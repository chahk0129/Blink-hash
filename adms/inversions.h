#pragma once

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <limits.h>


typedef struct r_s {
    int* arr;
    size_t offset;
    size_t inversions;
    size_t size;
} array_t ;

void print_array_structure(const array_t* array);

void enable_inv_count();

array_t count_inversions(array_t array);

size_t count_inversions_wrapper(const int* a, size_t size);

size_t getInvCount(const int arr[], size_t n);

