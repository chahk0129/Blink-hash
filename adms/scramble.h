#pragma once

#include <stdlib.h>
#include <stdio.h>
#include <memory.h>

void print_array(const void *arr, const size_t size, const size_t obj_size);

void scramble(void *array, const size_t length, size_t obj_size, size_t frame_width, size_t inversions, size_t stride);

void count_differences(const void* arr, const size_t size, const size_t obj_size);

void number_of_inversions(const void* arr, const size_t size, const size_t obj_size);
