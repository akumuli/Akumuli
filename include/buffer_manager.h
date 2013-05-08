#pragma once

namespace IO
{

struct IOBuffer
{
    void* address;
    size_t length;
};


/** Interface to buffer manager
 */
struct IBufferManager
{
    //! Create new buffer
    IOBuffer make();
    //! Return buffer back
    void recycle(IOBuffer buffer);
};


/** Buffer manager factory.
 */
struct BufferManagerFactory
{
    enum BufferType {
        MMF,  //< Memory mapped file
        Memory //< Memory allocation
    };

    /** Create new buffer manager of some type.
     * There is two types:
     * - MMF - memory mapped files, `param` must contain path to file.
     * - Memory - memory allocation from OS, `param` can be null. 
     */
    IBufferManager* create_new(BufferType type, size_t page_size, const char* param);
};

}
