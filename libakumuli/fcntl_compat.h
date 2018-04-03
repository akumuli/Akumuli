/* -*- Mode: C++; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*-
 * ***** BEGIN LICENSE BLOCK *****
 * Version: MPL 1.1/GPL 2.0/LGPL 2.1
 *
 * The contents of this file are subject to the Mozilla Public License Version
 * 1.1 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.mozilla.org/MPL/
 *
 * Software distributed under the License is distributed on an "AS IS" basis,
 * WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
 * for the specific language governing rights and limitations under the
 * License.
 *
 * The Original Code is Mozilla code.
 *
 * The Initial Developer of the Original Code is
 * Mozilla Foundation.
 * Portions created by the Initial Developer are Copyright (C) 2010
 * the Initial Developer. All Rights Reserved.
 *
 * Contributor(s):
 *   Taras Glek <tglek@mozilla.com>
 *
 * Alternatively, the contents of this file may be used under the terms of
 * either the GNU General Public License Version 2 or later (the "GPL"), or
 * the GNU Lesser General Public License Version 2.1 or later (the "LGPL"),
 * in which case the provisions of the GPL or the LGPL are applicable instead
 * of those above. If you wish to allow use of your version of this file only
 * under the terms of either the GPL or the LGPL, and not to allow others to
 * use your version of this file under the terms of the MPL, indicate your
 * decision by deleting the provisions above and replace them with the notice
 * and other provisions required by the GPL or the LGPL. If you do not delete
 * the provisions above, a recipient may use your version of this file under
 * the terms of any one of the MPL, the GPL or the LGPL.
 *
 * ***** END LICENSE BLOCK ***** */

#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>

// created from the OSX-specific code from Mozilla's mozilla::fallocation() function
// of which the licensing information is copied above.
// Adaptation (C) 2015,2016 R.J.V. Bertin


// From Linux `man posix_fallocate`:
// DESCRIPTION
//        The function posix_fallocate() ensures that disk space is allocated for
//        the file referred to by the descriptor fd for the bytes  in  the  range
//        starting  at  offset  and continuing for len bytes.  After a successful
//        call to posix_fallocate(), subsequent writes to bytes in the  specified
//        range are guaranteed not to fail because of lack of disk space.
// 
//        If  the  size  of  the  file  is less than offset+len, then the file is
//        increased to this size; otherwise the file size is left unchanged.

// From OS X man fcntl:
//      F_PREALLOCATE      Preallocate file storage space. Note: upon success, the space
//                         that is allocated can be the same size or larger than the space
//                         requested.
//      The F_PREALLOCATE command operates on the following structure:
//              typedef struct fstore {
//                  u_int32_t fst_flags;      /* IN: flags word */
//                  int       fst_posmode;    /* IN: indicates offset field */
//                  off_t     fst_offset;     /* IN: start of the region */
//                  off_t     fst_length;     /* IN: size of the region */
//                  off_t     fst_bytesalloc; /* OUT: number of bytes allocated */
//              } fstore_t;
//      The flags (fst_flags) for the F_PREALLOCATE command are as follows:
//            F_ALLOCATECONTIG   Allocate contiguous space.
//            F_ALLOCATEALL      Allocate all requested space or no space at all.
//      The position modes (fst_posmode) for the F_PREALLOCATE command indicate how to use
//      the offset field.  The modes are as follows:
//            F_PEOFPOSMODE   Allocate from the physical end of file.
//            F_VOLPOSMODE    Allocate from the volume offset.

// From OS X man ftruncate:
// DESCRIPTION
//      ftruncate() and truncate() cause the file named by path, or referenced by fildes, to
//      be truncated (or extended) to length bytes in size. If the file size exceeds length,
//      any extra data is discarded. If the file size is smaller than length, the file 
//      extended and filled with zeros to the indicated length.  The ftruncate() form requires
//      the file to be open for writing.
//      Note: ftruncate() and truncate() do not modify the current file offset for any open
//      file descriptions associated with the file.

#ifdef __APPLE__
static int posix_fallocate(int fd, off_t offset, off_t len)
{
    off_t c_test;
    int ret;
    if (!__builtin_saddll_overflow(offset, len, &c_test)) {
        fstore_t store = {F_ALLOCATECONTIG, F_PEOFPOSMODE, 0, offset + len};
        // Try to get a continous chunk of disk space
        ret = fcntl(fd, F_PREALLOCATE, &store);
        if (ret < 0) {
            // OK, perhaps we are too fragmented, allocate non-continuous
            store.fst_flags = F_ALLOCATEALL;
            ret = fcntl(fd, F_PREALLOCATE, &store);
            if (ret < 0) {
                return ret;
            }
        }
        ret = ftruncate(fd, offset + len);
    } else {
        // offset+len would overflow.
        ret = -1;
    }
    return ret;
}
#endif
