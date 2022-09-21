#include <time.h>
#include "scramble.h"

int sranded = 0;

void print_array(const void *arr, const size_t size, const size_t obj_size) {
    if (obj_size == sizeof(int)) {
        int *data = (int *) arr;
        for (size_t i = 0; i < size; i++) {
            printf("%d ", data[i]);
        }
    } else if (obj_size == sizeof(size_t)) {
        size_t *data = (size_t *) arr;
        for (size_t i = 0; i < size; i++) {
            printf("%lu ", data[i]);
        }
    }
    printf("\n");
}

void scramble(void *array, const size_t length, size_t obj_size, size_t frame_width, size_t inversions, size_t stride) {
    if(!sranded) {
        srand(time(NULL));
        sranded = 1;
    }

    size_t f_start = 0;
    size_t f_end = length;

    for (size_t center = 0; center < length; center += stride) {
        if (frame_width >= length) {
            f_start = 0;
            f_end = length;
        } else {
            if (center < frame_width) {
                f_start = 0;
                f_end = frame_width;
            }
            if (center + frame_width > length) {
                f_end = length;
                f_start = length - frame_width;
            }
            if (frame_width <= center && center <= length - frame_width) {
                f_end = center + frame_width;
                f_start = center - frame_width;
            }

            size_t arr_size = (f_end - f_start) * obj_size;
            unsigned char l[arr_size];
            unsigned char temp[obj_size];
            memcpy(l, (unsigned char*)array + f_start * obj_size, arr_size);
            for (size_t k = 0; k < inversions; k++) {
                size_t i = rand() % frame_width;
                size_t j = i;
                while (frame_width > 1 && i == j)
                    j = rand() % frame_width;

                // Swap both values
                memcpy(temp, l + i * obj_size, obj_size);
                memcpy(l + i * obj_size, l + j * obj_size, obj_size);
                memcpy(l + j * obj_size, temp, obj_size);
            }
            memcpy((unsigned char*)array + f_start * obj_size, l, arr_size);
        }
    }
}
