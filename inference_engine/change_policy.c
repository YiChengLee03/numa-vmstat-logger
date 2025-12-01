#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h> 
#include <sys/syscall.h>

#define __NR_my_new_syscall 470

int main(int argc, char* argv[]) {
    if (argc < 4) {
        fprintf(stderr, "Usage: %s <pid> <mode> <nodecount> \n", argv[0]);
        return 1;
    }

    pid_t pid = atoi(argv[1]);
    int mode = atoi(argv[2]);
    int nodecount = atoi(argv[3]);

    // Allocate nmask: enough to hold nodecount bits
    unsigned long* nmask = calloc(
        (nodecount + sizeof(unsigned long) * 8 - 1) / (sizeof(unsigned long) * 8),
        sizeof(unsigned long)
    );

    if (!nmask) {
        perror("calloc");
        return 1;
    }

    // Set all bits for allowed nodes
    for (int i = 0; i < nodecount; i++) {
        nmask[i / (sizeof(unsigned long) * 8)] |= 1UL << (i % (sizeof(unsigned long) * 8));
    }

    long ret = syscall(__NR_my_new_syscall, pid, mode, nmask, nodecount);
    if (ret != 0) {
        perror("syscall");
    }
    else {
        printf("Syscall succeeded\n");
    }

    free(nmask);
    return 0;
}