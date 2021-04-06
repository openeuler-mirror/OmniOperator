#include "stdio.h"
int callee();

int caller() {
    printf("I am caller");
    return callee();
}