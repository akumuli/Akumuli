#include <iostream>
#include <cassert>
#include "page.h"
#include <random>
#include <boost/timer.hpp>

using namespace Akumuli;

int main()
{
    size_t page_size = 1024*1024*1024;;
    void* underlying_storage = malloc(page_size);
    PageHeader* page = new(underlying_storage) PageHeader(PageType::Leaf, 0, page_size);
    boost::timer fill_timer;
    unsigned max_index = 1024*1024*1024;
    for (unsigned i = 0; i < max_index; i++)
    {
        char buf[1000];
        //int rnd = rand() % 444;
        TimeStamp tm;
        tm.precise = i/10000;// + rnd;
        auto entry = new (buf) Entry(max_index - i , tm, sizeof(Entry));
        entry->value[0] = i;
        auto status = page->add_entry(*entry);
        if (status == PageHeader::AddStatus::Overflow)
        {
            double es = fill_timer.elapsed();
            std::cout << "break at " << i << " and " << es << std::endl;
            max_index = i;
            break;
        }
    }
    fill_timer.restart();
    page->sort();
    double es = fill_timer.elapsed();
    std::cout << "done in " << es << std::endl;
    // Check!
    for (unsigned i = 0; i < max_index; i++)
    {
        char buf[1000];
        TimeStamp tm;
        auto entry = new (buf) Entry(i, tm, sizeof(Entry));
        page->copy_entry(i, entry);
        bool correct =
            (entry->length == sizeof(Entry)) &&
            //(entry->param_id == max_index - i) &&
            (entry->time.precise == i/10000) &&
            //(entry->value[0] == i);
            true;
        if (!correct) {
            std::cout << "invalid value at " << i << std::endl;
            //break;
        }
    }
    return 0;
}
