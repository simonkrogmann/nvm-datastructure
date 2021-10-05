#pragma once

#include <random>
#include <array>

#include "utree.h"

const size_t padding_size = 64;

struct Data {

    int64_t primary;
    int64_t secondary;
    // just to increase size
    std::array<int64_t, padding_size> padding;
};


void experiment()
{
    std::mt19937 rng;
    std::uniform_int_distribution<int64_t> data_dist(0, 100'000'000ull);
    btree<Data> primary;
    btree<Data*> secondary;

    std::vector<int64_t> primary_keys;
    std::vector<int64_t> secondary_keys;


    for (int i = 0; i < 2'000'000; ++i)
    {
        Data d {.primary = data_dist(rng), .secondary = data_dist(rng)};
        for (auto & el : d.padding)
        {
            el = data_dist(rng);
        }
        auto found = primary.search(d.primary);
        if (found == nullptr)
        {
            auto inserted = primary.insert(d.primary, d);
            secondary.insert(inserted->secondary, inserted);
            primary_keys.push_back(inserted->primary);
            secondary_keys.push_back(inserted->secondary);
        }
    }
    std::shuffle(primary_keys.begin(), primary_keys.end(), rng);
    std::shuffle(secondary_keys.begin(), secondary_keys.end(), rng);

    std::cout << "Created primary and secondary index" << std::endl;
    std::cout << "Now starting experiments..." << std::endl;

    int counter = 0;
    int counter2 = 0;
    int repeats = 1'000'000;
    for (int i = 0; i < repeats; ++i)
    {
        auto ptr = primary.search(primary_keys[i]);
        assert(ptr != nullptr);
        if (ptr->padding[10] < 1'000)
        {
            ++counter;
        }
    }
    std::cout << "Counters: " << counter << ", " << counter2 << std::endl;
    for (int i = 0; i < repeats; ++i)
    {
        auto ptr = secondary.search(secondary_keys[i]);
        assert(ptr != nullptr);
        if ((*ptr)->padding[10] < 1'000)
        {
            ++counter2;
        }
    }
    std::cout << "Counters: " << counter << ", " << counter2 << std::endl;
    for (int i = 0; i < repeats; ++i)
    {
        auto ptr = primary.search(primary_keys[i] + 1);
        if (ptr && ptr->padding[10] < 1'000)
        {
            ++counter;
        }
    }
    std::cout << "Counters: " << counter << ", " << counter2 << std::endl;
    for (int i = 0; i < repeats; ++i)
    {
        auto ptr = secondary.search(secondary_keys[i] + 1);
        if (ptr && (*ptr)->padding[10] < 1'000)
        {
            ++counter2;
        }
    }
    std::cout << "Counters: " << counter << ", " << counter2 << std::endl;
}
