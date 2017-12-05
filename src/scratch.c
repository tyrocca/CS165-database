#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main() {
    const char* hello = "Hello, World!";
    char* str = malloc(13 * sizeof(char)); // "Hello, World!" is 13 characters long

    for (int i = 0; i < 13; i++) {
        str[i] = hello[i];
    }
    str[13] = 0; // Don't forget the terminating nul character

    printf("%s\n", str);

    return 0;
}
