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
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
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
