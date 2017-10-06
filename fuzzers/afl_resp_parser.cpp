#include <fstream>
#include <iostream>
#include "akumuli.h"
#include "resp.h"

using namespace Akumuli;

int main(int argc, char** argv) {
    if (argc == 1) {
        return 1;
    }
    std::string file_name(argv[1]);
    std::fstream input(file_name, std::ios::binary|std::ios::in|std::ios::out);
    std::string content((std::istreambuf_iterator<char>(input)), std::istreambuf_iterator<char>());
    MemStreamReader bstream(content.data(), content.size());
    RESPStream stream(&bstream);
    char strbuffer[RESPStream::STRING_LENGTH_MAX];
    char bulkbuffer[RESPStream::BULK_LENGTH_MAX];
    try {
        while(bstream.is_eof() == false) {
            auto type = stream.next_type();
            switch(type) {
            case RESPStream::INTEGER:
                stream.read_int();
                break;
            case RESPStream::STRING:
                stream.read_string(strbuffer, RESPStream::STRING_LENGTH_MAX);
                break;
            case RESPStream::BULK_STR:
                stream.read_bulkstr(bulkbuffer, RESPStream::BULK_LENGTH_MAX);
                break;
            case RESPStream::ARRAY:
                stream.read_array_size();
                break;
            case RESPStream::_BAD:
                return 0;
            default:
                break;
            };
        }
    } catch (const StreamError&) {
        // Ignore format errors
    }
}
