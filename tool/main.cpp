/**
 *  Akumuli-tool - simple storage manager and viewer with cli interface for akumuli.
 *
 *  Functions:
 *  - create storage;
 *  - delete storage;
 *  - incremental backup;
 *  - resize storage;
 *  - view stats;
 *
 * Copyright (c) 2013 Eugene Lazin <4lazin@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#include <iostream>

#include <akumuli.h>
#include <apr.h>
#include <apr_getopt.h>

void show_help() {
    std::cout << "Akumuli-tool - simple storage manager and viewer with cli interface for akumuli." << std::endl;
    // TODO: show help
}

int main(int argc, char** argv) {
    apr_status_t status;
    apr_pool_t *pool;
    static const apr_getopt_option_t options[] = {
        { "help", 'h', FALSE, "show help" },
        { "path", 'p', TRUE, "path to akumuli storage" },
        // Create
        { "create", 'c', FALSE, "show help" },
        { "volumes", 'v', TRUE, "number of volumes" },
        // Delete
        { "delete", 'd', FALSE, "delete storage" },
        // Backup (TODO)
        { "backup", 'b', FALSE, "backup storage" },
        // Resize (TODO)
        { "resize", 'r', FALSE, "resize storage" },
        { "stats", 's', FALSE, "view stats" },
        { NULL, 0, 0, NULL },
    };
    apr_getopt_t *opt;
    int optch;
    const char *optarg;

    apr_initialize();
    apr_pool_create(&pool, NULL);

    /* initialize apr_getopt_t */
    apr_getopt_init(&opt, pool, argc, argv);

    /* parse the all options based on opt_option[] */
    while ((status = apr_getopt_long(opt, options, &optch, &optarg)) == APR_SUCCESS) {
        switch (optch) {
        case 'p':
            std::cout << "Path: " << optarg << std::endl;
            break;
        case 'c':
            std::cout << "Create storage " << std::endl;
            break;
        case 'h':
            show_help();
            break;
        default:
            break;
        }
    }
    if (status != APR_EOF) {
        std::cout << "Invalid arguments" << std::endl;
        show_help();
    }
    apr_terminate();
    return 0;
};
