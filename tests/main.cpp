#include <iostream>
#include "recorder.h"

int main()
{
    Config conf;
    conf.path_to_file = (char*)"null";
    conf.debug_mode = 0;
    conf.page_size = 4096;
    auto db = open_database(conf);
    db->flush(db);
    close_database(db);
    std::cout << "OK" << std::endl;
    return 0;
}
