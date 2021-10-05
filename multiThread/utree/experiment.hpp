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


    for (int i = 0; i < 100'000; ++i)
    {
        Data d {.primary = data_dist(rng), .secondary = data_dist(rng)};
        for (auto & el : d.padding)
        {
            el = data_dist(rng);
        }
        auto found = primary.search(d.primary);
        if (found != nullptr)
        {
            auto inserted = primary.insert(d.primary, d);
            secondary.insert(inserted->secondary, inserted);
        }
    }
}
