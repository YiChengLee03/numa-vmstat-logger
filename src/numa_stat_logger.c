#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h> 

#include <sys/types.h> 
#include <sys/wait.h>

struct node_meminfo {
    unsigned mem_total;
    unsigned mem_used;
};

struct node_vmstat {
    unsigned nr_free_pages;
    unsigned numa_hit;
    unsigned numa_miss;
    unsigned numa_foreign;
    unsigned numa_interleave;
    unsigned numa_local;
    unsigned numa_other;
};

struct sys_vmstat {
    unsigned numa_pte_updates;
    unsigned numa_huge_pte_updates;
    unsigned numa_pages_migrated;
    unsigned pgmigrate_success;
    unsigned pgmigrate_fail;
    unsigned thp_migration_success;
    unsigned thp_migration_fail;
    unsigned thp_migration_split;
};

static void parse_node_meminfo(struct node_meminfo* nm,
    const char* path,
    int nodeID)
{
    FILE* fp = fopen(path, "r");
    if (!fp) return;

    char line[256];
    char memtotal_key[32], memfree_key[32];
    snprintf(memtotal_key, sizeof(memtotal_key), "Node %d MemTotal:", nodeID);
    snprintf(memfree_key, sizeof(memfree_key), "Node %d MemFree:", nodeID);

    int found_total = 0, found_free = 0;

    while (fgets(line, sizeof(line), fp)) {
        unsigned val;

        if (!found_total && strstr(line, memtotal_key)) {
            if (sscanf(line + strlen(memtotal_key), "%u", &val) == 1) {
                nm->mem_total = val;
                found_total = 1;
            }
        }
        else if (!found_free && strstr(line, memfree_key)) {
            if (sscanf(line + strlen(memfree_key), "%u", &val) == 1) {
                nm->mem_used = nm->mem_total - val;
                found_free = 1;
            }
        }

        if (found_total && found_free)
            break;
    }

    fclose(fp);
}

static void parse_node_vmstat(struct node_vmstat* nv,
    const char* path)
{
    FILE* fp = fopen(path, "r");
    if (!fp) return;

    char key[128];
    unsigned val;
    int found_count = 0;

    int f_nr_free_pages = 0;
    int f_numa_hit = 0;
    int f_numa_miss = 0;
    int f_numa_foreign = 0;
    int f_numa_interleave = 0;
    int f_numa_local = 0;
    int f_numa_other = 0;

    const int TOTAL_FIELDS = 7;

    while (fscanf(fp, "%127s %u", key, &val) == 2) {

        if (!f_nr_free_pages && strcmp(key, "nr_free_pages") == 0) {
            nv->nr_free_pages = val;
            f_nr_free_pages = 1;
            found_count++;
        }
        else if (!f_numa_hit && strcmp(key, "numa_hit") == 0) {
            nv->numa_hit = val;
            f_numa_hit = 1;
            found_count++;
        }
        else if (!f_numa_miss && strcmp(key, "numa_miss") == 0) {
            nv->numa_miss = val;
            f_numa_miss = 1;
            found_count++;
        }
        else if (!f_numa_foreign && strcmp(key, "numa_foreign") == 0) {
            nv->numa_foreign = val;
            f_numa_foreign = 1;
            found_count++;
        }
        else if (!f_numa_interleave && strcmp(key, "numa_interleave") == 0) {
            nv->numa_interleave = val;
            f_numa_interleave = 1;
            found_count++;
        }
        else if (!f_numa_local && strcmp(key, "numa_local") == 0) {
            nv->numa_local = val;
            f_numa_local = 1;
            found_count++;
        }
        else if (!f_numa_other && strcmp(key, "numa_other") == 0) {
            nv->numa_other = val;
            f_numa_other = 1;
            found_count++;
        }

        if (found_count == TOTAL_FIELDS)
            break;
    }

    fclose(fp);
}

static void parse_sys_vmstat(struct sys_vmstat* sv,
    const char* path)
{
    FILE* fp = fopen(path, "r");
    if (!fp) return;

    char key[128];
    unsigned val;
    int found_count = 0;

    int f_numa_pte_updates = 0;
    int f_numa_huge_pte_updates = 0;
    int f_numa_pages_migrated = 0;
    int f_pgmigrate_success = 0;
    int f_pgmigrate_fail = 0;
    int f_thp_migration_success = 0;
    int f_thp_migration_fail = 0;
    int f_thp_migration_split = 0;

    const int TOTAL_FIELDS = 8;

    while (fscanf(fp, "%127s %u", key, &val) == 2) {

        if (!f_numa_pte_updates && strcmp(key, "numa_pte_updates") == 0) {
            sv->numa_pte_updates = val;
            f_numa_pte_updates = 1;
            found_count++;
        }
        else if (!f_numa_huge_pte_updates && strcmp(key, "numa_huge_pte_updates") == 0) {
            sv->numa_huge_pte_updates = val;
            f_numa_huge_pte_updates = 1;
            found_count++;
        }
        else if (!f_numa_pages_migrated && strcmp(key, "numa_pages_migrated") == 0) {
            sv->numa_pages_migrated = val;
            f_numa_pages_migrated = 1;
            found_count++;
        }
        else if (!f_pgmigrate_success && strcmp(key, "pgmigrate_success") == 0) {
            sv->pgmigrate_success = val;
            f_pgmigrate_success = 1; found_count++;
        }
        else if (!f_pgmigrate_fail && strcmp(key, "pgmigrate_fail") == 0) {
            sv->pgmigrate_fail = val;
            f_pgmigrate_fail = 1;
            found_count++;
        }
        else if (!f_thp_migration_success && strcmp(key, "thp_migration_success") == 0) {
            sv->thp_migration_success = val;
            f_thp_migration_success = 1;
            found_count++;
        }
        else if (!f_thp_migration_fail && strcmp(key, "thp_migration_fail") == 0) {
            sv->thp_migration_fail = val;
            f_thp_migration_fail = 1;
            found_count++;
        }
        else if (!f_thp_migration_split && strcmp(key, "thp_migration_split") == 0) {
            sv->thp_migration_split = val;
            f_thp_migration_split = 1;
            found_count++;
        }

        if (found_count == TOTAL_FIELDS)
            break;
    }


    fclose(fp);
}

void write_csv_header(const char* filename, int numa_count) {
    FILE* fp = fopen(filename, "r");
    if (fp) {
        fclose(fp);
        return;
    }

    fp = fopen(filename, "w");
    if (!fp) { perror("fopen"); exit(1); }

    fprintf(fp, "timestamp");
    for (int i = 0; i < numa_count; i++)
        fprintf(fp, ",node_%d_mem_total,node_%d_mem_used", i, i);
    for (int i = 0; i < numa_count; i++)
        fprintf(fp, ",node_%d_nr_free_pages,node_%d_numa_hit,node_%d_numa_miss,node_%d_numa_foreign,node_%d_numa_interleave,node_%d_numa_local,node_%d_numa_other",
            i, i, i, i, i, i, i);

    const char* sys_cols[] = {
        "numa_pte_updates","numa_huge_pte_updates","numa_pages_migrated",
        "pgmigrate_success","pgmigrate_fail","thp_migration_success",
        "thp_migration_fail","thp_migration_split"
    };
    for (int i = 0; i < 8; i++)
        fprintf(fp, ",%s", sys_cols[i]);

    fprintf(fp, "\n");
    fclose(fp);
}

int main(int argc, char* argv[]) {
    if (argc < 4) {
        fprintf(stderr, "Usage: %s <numa_count> <interval_sec> (-d <duration_sec> | -r <command> [args...])\n", argv[0]);
        return 1;
    }

    int numa_count = atoi(argv[1]);
    if (numa_count <= 0) {
        fprintf(stderr, "Invalid numa_count: %d\n", numa_count);
        return 1;
    }

    double interval_sec = atof(argv[2]);
    if (interval_sec <= 0) {
        fprintf(stderr, "Invalid interval_sec: %f\n", interval_sec);
        return 1;
    }

    int use_duration = 0;
    int duration_sec = 0;
    int use_run = 0;
    char** run_argv = NULL;

    // parse mode
    if (strcmp(argv[3], "-d") == 0) {
        if (argc < 5) {
            fprintf(stderr, "Missing duration argument\n");
            return 1;
        }
        use_duration = 1;
        duration_sec = atoi(argv[4]);
        if (duration_sec <= 0) {
            fprintf(stderr, "Invalid duration_sec: %d\n", duration_sec);
            return 1;
        }
    }
    else if (strcmp(argv[3], "-r") == 0) {
        if (argc < 5) {
            fprintf(stderr, "Missing command to run\n");
            return 1;
        }
        use_run = 1;
        run_argv = &argv[4]; // points to command + args
    }
    else {
        fprintf(stderr, "Unknown mode: %s\n", argv[3]);
        return 1;
    }

    int iterations = 0;
    if (use_duration) {
        iterations = duration_sec / interval_sec;
    }

    struct node_meminfo* nm = malloc(sizeof(struct node_meminfo) * numa_count);
    struct node_vmstat* nv = malloc(sizeof(struct node_vmstat) * numa_count);
    struct sys_vmstat sv;

    if (!nm || !nv) {
        fprintf(stderr, "Failed to allocate memory for NUMA arrays\n");
        free(nm); free(nv);
        return 1;
    }

    const char* csv_file = "numa_stat_log.csv";
    write_csv_header(csv_file, numa_count);

    FILE* fp = fopen(csv_file, "a");
    if (!fp) {
        perror("fopen append");
        free(nm); free(nv);
        return 1;
    }

    pid_t child_pid = -1;
    int status = 0;
    if (use_run) {
        child_pid = fork();
        if (child_pid == 0) {
            // child: run the command
            execvp(run_argv[0], run_argv);
            perror("execvp failed");
            exit(1);
        }
        // parent continues to log
    }

    char meminfo_path[128], vmstat_path[128];

    for (int iter = 0; use_duration ? (iter < iterations) : 1; iter++) {
        // --- Parse all nodes ---
        for (int i = 0; i < numa_count; i++) {
            snprintf(meminfo_path, sizeof(meminfo_path),
                "/sys/devices/system/node/node%d/meminfo", i);
            parse_node_meminfo(&nm[i], meminfo_path, i);

            snprintf(vmstat_path, sizeof(vmstat_path),
                "/sys/devices/system/node/node%d/vmstat", i);
            parse_node_vmstat(&nv[i], vmstat_path);
        }
        parse_sys_vmstat(&sv, "/proc/vmstat");

        // --- Write CSV row ---
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        fprintf(fp, "%ld.%09ld", ts.tv_sec, ts.tv_nsec);

        for (int i = 0; i < numa_count; i++)
            fprintf(fp, ",%u,%u", nm[i].mem_total, nm[i].mem_used);

        for (int i = 0; i < numa_count; i++)
            fprintf(fp, ",%u,%u,%u,%u,%u,%u,%u",
                nv[i].nr_free_pages, nv[i].numa_hit, nv[i].numa_miss,
                nv[i].numa_foreign, nv[i].numa_interleave,
                nv[i].numa_local, nv[i].numa_other);

        fprintf(fp, ",%u,%u,%u,%u,%u,%u,%u,%u\n",
            sv.numa_pte_updates, sv.numa_huge_pte_updates, sv.numa_pages_migrated,
            sv.pgmigrate_success, sv.pgmigrate_fail,
            sv.thp_migration_success, sv.thp_migration_fail, sv.thp_migration_split);

        fflush(fp);

        // Sleep interval
        struct timespec ts_sleep = { 0, (long)(interval_sec * 1e9) };
        nanosleep(&ts_sleep, NULL);

        // Stop condition for run-executable mode
        if (use_run) {
            pid_t ret = waitpid(child_pid, &status, WNOHANG);
            if (ret != 0)
                break; // child finished
        }
    }

    // If run mode and child still exists, wait for it
    if (use_run)
        waitpid(child_pid, &status, 0);

    fclose(fp);
    free(nm);
    free(nv);

    return 0;
}
