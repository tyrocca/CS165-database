#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main() {
    int max[400];
    memset((void*) max, 0, sizeof(int) * 400);
    max[399] = 200;
    int starting_value = 0;
    while (max[starting_value++] < 100)
    printf("Done, %d", starting_value);
    return 0;
}
