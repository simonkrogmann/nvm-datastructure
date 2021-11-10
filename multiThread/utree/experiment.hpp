#pragma once

#include <random>
#include <array>
#include <unordered_set>
#include <typeinfo>

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

    std::vector<std::pair<Data, Data *>> data;
    std::unordered_set<int64_t> inserted_keys;
    std::vector<int64_t> primary_keys;
    std::vector<int64_t> secondary_keys;
    while (data.size() < 2'000'000)
    {
        Data d {.primary = data_dist(rng), .secondary = data_dist(rng)};
        for (auto & el : d.padding)
        {
            el = data_dist(rng);
        }
        if (inserted_keys.find(d.primary) == inserted_keys.end())
        {
            inserted_keys.insert(d.primary);
            data.push_back({d, nullptr});
            primary_keys.push_back(d.primary);
            secondary_keys.push_back(d.secondary);
        }
    }



    auto start = std::chrono::high_resolution_clock::now();

    for (auto & [el, loc] : data)
    {
        auto inserted = primary.insert(el.primary, el);
        loc = inserted;
    }

    auto end = std::chrono::high_resolution_clock::now();
    int64_t elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();

    std::cout << "Primary inserts" << std::endl
        << "  Total time = " << elapsed << " ns" << std::endl
        << "  Average time = " << (elapsed / static_cast<float>(data.size())) << " ns" << std::endl;

    sleep(5);

    start = std::chrono::high_resolution_clock::now();
    for (const auto & [el, loc] : data)
    {
        secondary.insert(el.secondary, loc);
    }

    end = std::chrono::high_resolution_clock::now();
    elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();

    std::cout << "Secondary inserts" << std::endl
        << "  Total time = " << elapsed << " ns" << std::endl
        << "  Average time = " << (elapsed / static_cast<float>(data.size())) << " ns" << std::endl;

    std::shuffle(primary_keys.begin(), primary_keys.end(), rng);
    std::shuffle(secondary_keys.begin(), secondary_keys.end(), rng);

    std::cout << "Created primary and secondary index" << std::endl;
    std::cout << "Now starting experiments..." << std::endl;

    int counter = 0;
    int counter2 = 0;
    int repeats = 1'000'000;

    start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < repeats; ++i)
    {
        auto ptr = primary.search(primary_keys[i]);
        assert(ptr != nullptr);
        if (ptr->padding[10] < 1'000)
        {
            ++counter;
        }
    }
    end = std::chrono::high_resolution_clock::now();
    elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();

    std::cout << "Primary search hit" << std::endl
        << "  Total time = " << elapsed << " ns" << std::endl
        << "  Average time = " << (elapsed / static_cast<float>(data.size())) << " ns" << std::endl;

    sleep(5);

    start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < repeats; ++i)
    {
        auto ptr = secondary.search(secondary_keys[i]);
        assert(ptr != nullptr);
        if ((*ptr)->padding[10] < 1'000)
        {
            ++counter2;
        }
    }
    end = std::chrono::high_resolution_clock::now();
    elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();

    std::cout << "Secondary search hit" << std::endl
        << "  Total time = " << elapsed << " ns" << std::endl
        << "  Average time = " << (elapsed / static_cast<float>(data.size())) << " ns" << std::endl;

    sleep(5);

    start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < repeats; ++i)
    {
        auto ptr = primary.search(primary_keys[i] + 1);
        if (ptr && ptr->padding[10] < 1'000)
        {
            ++counter;
        }
    }
    end = std::chrono::high_resolution_clock::now();
    elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();

    std::cout << "Primary search miss" << std::endl
        << "  Total time = " << elapsed << " ns" << std::endl
        << "  Average time = " << (elapsed / static_cast<float>(data.size())) << " ns" << std::endl;

    sleep(5);

    start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < repeats; ++i)
    {
        auto ptr = secondary.search(secondary_keys[i] + 1);
        if (ptr && (*ptr)->padding[10] < 1'000)
        {
            ++counter2;
        }
    }
    end = std::chrono::high_resolution_clock::now();
    elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();

    std::cout << "Secondary search miss" << std::endl
        << "  Total time = " << elapsed << " ns" << std::endl
        << "  Average time = " << (elapsed / static_cast<float>(data.size())) << " ns" << std::endl;

    std::cout << "Counters: " << counter << ", " << counter2 << std::endl;
}
