#pragma once

#include <random>
#include <array>
#include <unordered_set>
#include <typeinfo>

#include "utree.h"

const size_t padding_size = 64;
std::uniform_int_distribution<uint64_t> data_dist(0, 100'000'000ull);
std::mt19937 rng;

namespace std
{
    template<typename T, size_t N>
    struct hash<array<T, N> >
    {
        typedef array<T, N> argument_type;
        typedef size_t result_type;

        result_type operator()(const argument_type& a) const
        {
            hash<T> hasher;
            result_type h = 0;
            for (result_type i = 0; i < N; ++i)
            {
                h = h * 31 + hasher(a[i]);
            }
            return h;
        }
    };
}

template <size_t S>
void randomize(std::array<uint64_t, S> & array)
{
    for (auto & el : array)
    {
        el = data_dist(rng);
    }
}

struct Data {

    entry_key_t primary;
    entry_key_t secondary;
    // just to increase size
    std::array<uint64_t, padding_size> padding;
};


void experiment()
{
    btree<Data> primary;
    btree<Data*> secondary;

    std::vector<std::pair<Data, Data *>> data;
    std::unordered_set<entry_key_t> primary_set;
    std::unordered_set<entry_key_t> secondary_set;
    std::vector<entry_key_t> primary_keys;
    std::vector<entry_key_t> secondary_keys;
    while (data.size() < 2'000'000)
    {
        Data d;
        randomize(d.primary);
        randomize(d.secondary);
        randomize(d.padding);
        if (primary_set.find(d.primary) == primary_set.end())
        {
            primary_set.insert(d.primary);
            secondary_set.insert(d.secondary);
            data.push_back({d, nullptr});
            primary_keys.push_back(d.primary);
            secondary_keys.push_back(d.secondary);
        }
    }
    std::vector<entry_key_t> not_present_primary;
    while (not_present_primary.size() < 1'000'000)
    {
        entry_key_t key;
        randomize(key);
        if (primary_set.find(key) == primary_set.end())
        {
            not_present_primary.push_back(key);
        }
    }
    std::vector<entry_key_t> not_present_secondary;
    while (not_present_secondary.size() < 1'000'000)
    {
        entry_key_t key;
        randomize(key);
        if (secondary_set.find(key) == secondary_set.end())
        {
            not_present_secondary.push_back(key);
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
        auto ptr = primary.search(not_present_primary[i]);
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
        auto ptr = secondary.search(not_present_secondary[i]);
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
