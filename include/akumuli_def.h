#ifndef AKUMULI_DEF_H
#define AKUMULI_DEF_H

// Write status

//! Succesfull write
#define AKU_WRITE_STATUS_SUCCESS  0
//! Page overflow during write
#define AKU_WRITE_STATUS_OVERFLOW 1
//! Invalid input
#define AKU_WRITE_STATUS_BAD_DATA 2


// Search FSM state table

#define AKU_CURSOR_START          0
#define AKU_CURSOR_SEARCH         1
#define AKU_CURSOR_SCAN_BACKWARD  2
#define AKU_CURSOR_SCAN_FORWARD   3
#define AKU_CURSOR_COMPLETE       4
#define AKU_CURSOR_ERROR          5


// Search error codes

//! No error
#define AKU_SEARCH_SUCCESS        0
//! Can't find result
#define AKU_SEARCH_ENOT_FOUND     1
//! Invalid arguments
#define AKU_SEARCH_EBAD_ARG       2

// Cursor directions
#define AKU_CURSOR_DIR_FORWARD    0
#define AKU_CURSOR_DIR_BACKWARD   1


// Different tune parameters
#define AKU_INTERPOLATION_SEARCH_CUTOFF 0x100

#endif
