#pragma once

#include <random>
#include <array>
#include <unordered_set>
#include <typeinfo>
#include <map>

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

int64_t time(std::function<void()> f)
{
    sleep(1);
    auto start = std::chrono::high_resolution_clock::now();
    f();
    auto end = std::chrono::high_resolution_clock::now();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
}


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

    const auto primary_insert_time = time([&](){
        for (auto & [el, loc] : data)
        {
            auto inserted = primary.insert(el.primary, el);
            loc = inserted;
        }
    }) / static_cast<float>(data.size());

    const auto secondary_insert_time = time([&](){
        for (const auto & [el, loc] : data)
        {
            secondary.insert(el.secondary, loc);
        }
    }) / static_cast<float>(data.size());


    std::shuffle(primary_keys.begin(), primary_keys.end(), rng);
    std::shuffle(secondary_keys.begin(), secondary_keys.end(), rng);

    const auto primary_dram = primary.getMemoryUsed();
    const auto secondary_dram = secondary.getMemoryUsed();
    const auto primary_nvram = primary.getPersistentMemoryUsed();
    const auto secondary_nvram = secondary.getPersistentMemoryUsed();


    int counter = 0;
    int counter2 = 0;
    int repeats = 1'000'000;

    const auto primary_hit = time([&](){
        for (int i = 0; i < repeats; ++i)
        {
            auto ptr = primary.search(primary_keys[i]);
            assert(ptr != nullptr);
        }
    }) / static_cast<float>(repeats);

    const auto secondary_hit = time([&](){
        for (int i = 0; i < repeats; ++i)
        {
            auto ptr = secondary.search(secondary_keys[i]);
            assert(ptr != nullptr);
        }
    }) / static_cast<float>(repeats);

    const auto primary_miss = time([&](){
        for (int i = 0; i < repeats; ++i)
        {
            auto ptr = primary.search(not_present_primary[i]);
        }
    }) / static_cast<float>(repeats);

    const auto secondary_miss = time([&](){
        for (int i = 0; i < repeats; ++i)
        {
            auto ptr = secondary.search(not_present_secondary[i]);
        }
    }) / static_cast<float>(repeats);

    std::map<size_t, float> primary_scan;
    std::map<size_t, float> secondary_scan;
    for (auto width : {10, 100, 1000})
    {
        primary_scan[width] = time([&](){
            for (int i = 0; i < repeats / 10; ++i)
            {
                auto res = primary.scan(primary_keys[i], width);
            }
        }) / static_cast<float>(repeats / 10);

        secondary_scan[width] = time([&](){
            for (int i = 0; i < repeats / 10; ++i)
            {
                auto res = secondary.secondaryScan(secondary_keys[i], width);
            }
        }) / static_cast<float>(repeats / 10);
    }

    std::cout << "Times in ns, storage in bytes" << std::endl;
    std::cout
        << "Key Size, Row Size, "
        << "Primary insert, Secondary insert, "
        << "Primary search hit, Secondary search hit, Primary search miss, Secondary search miss, "
        << "Primary (DRAM), Secondary (DRAM), Primary (NVRAM), Secondary (NVRAM),"
        << "PrimaryScan10, PrimaryScan100, PrimaryScan1000,"
        << "SecondaryScan10, SecondaryScan100, SecondaryScan1000,"
        << std::endl;

    std::cout
        << sizeof(entry_key_t) << "," << sizeof(Data) << ","
        << primary_insert_time << "," << secondary_insert_time << ","
        << primary_hit << "," << secondary_hit << "," << primary_miss << "," << secondary_miss << ","
        << primary_dram << "," << secondary_dram << "," << primary_nvram << "," << secondary_nvram << ","
        << primary_scan[10] << "," << primary_scan[100] << "," << primary_scan[1000] << ","
        << secondary_scan[10] << "," << secondary_scan[100] << "," << secondary_scan[1000] << ","
        << std::endl;
}
