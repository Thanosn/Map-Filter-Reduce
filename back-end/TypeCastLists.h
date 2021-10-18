#pragma once

#include <vector>
#include <string>
#include <queue>
#include <map>
#include <sstream>
#include <utility>

struct Maps
{
    const std::string resultedType = ""; //String for now enum later?
    void *function = nullptr;            //As returned from dlsym
};

//Takes a Vector<U> and returns a Vector<T> based on the types of T and U
template <typename T, typename U, typename std::enable_if<std::is_same<T, U>::value>::type * = nullptr>
const std::vector<T> typeCastList(const std::vector<U> &list)
{
    return std::move(list);
}

template <typename T, typename U, typename std::enable_if<!std::is_same<T, U>::value && !std::is_same<U, std::string>::value && !std::is_same<T, std::string>::value>::type * = nullptr>
const std::vector<T> typeCastList(const std::vector<U> &list)
{
    const std::vector<T> newList(begin(list), end(list));
    return newList;
}

template <typename T, typename U, typename std::enable_if<!std::is_same<T, U>::value && std::is_same<T, std::string>::value>::type * = nullptr>
const std::vector<T> typeCastList(const std::vector<U> &list)
{
    std::vector<T> newList;
    newList.reserve(list.size());
    for (auto &key : list)
    {
        newList.push_back(std::to_string(key));
    }
    return newList;
}

template <typename T, typename U, typename std::enable_if<!std::is_same<T, U>::value && std::is_same<U, std::string>::value>::type * = nullptr>
const std::vector<T> typeCastList(const std::vector<U> &list)
{
    std::vector<T> newList;
    newList.reserve(list.size());
    std::stringstream tmp;
    for (auto &str : list)
    {
        tmp.str(str);
        T t;
        tmp >> t;
        newList.push_back(t);
        tmp.clear();
    }
    return newList;
}

template <typename T>
class RDD;

namespace RDDParallelize
{
    //Takes a Vector and returns a RDD from it
    template <typename T, typename U>
    inline RDD<T> parallelize(const std::vector<U> &list)
    {
        RDD<T> rdd(typeCastList<T, U>(list));
        return rdd;
    }

    //Take two Vectors and returns a RDD<pair> from them
    template <typename T, typename U, typename K, typename L>
    inline RDD<std::pair<T, U>> parallelizePair(const std::vector<K> &list1, const std::vector<L> &list2)
    {
        RDD<std::pair<T, U>> rdd(typeCastList<T, K>(list1), typeCastList<U, L>(list2));
        return rdd;
    }
};

//Create a new RDD and parse the rest of the maps
template <typename NEWTYPE, typename OLDTYPE>
const std::vector<std::string> parseMapsCastRDD(std::vector<OLDTYPE> &oldKeys, std::queue<Maps> &maps, const std::string newType)
{
    auto newRDD = RDDParallelize::parallelize<NEWTYPE, OLDTYPE>(oldKeys);
    newRDD.Type = newType;
    oldKeys.clear();
    oldKeys.shrink_to_fit();
    return newRDD.parseMaps(maps);
}

//Create a new RDD<pair> and parse the rest of the maps
template <typename NEWTYPE1, typename NEWTYPE2, typename OLDTYPE1, typename OLDTYPE2>
const std::vector<std::string> parseMapsCastRDDPair(std::vector<OLDTYPE1> &list1, std::vector<OLDTYPE2> &list2, std::queue<Maps> &maps, const std::string newType)
{
    auto newRDD = RDDParallelize::parallelizePair<NEWTYPE1, NEWTYPE2, OLDTYPE1, OLDTYPE2>(list1, list2);
    newRDD.Type = newType;
    list1.clear();
    list1.shrink_to_fit();
    list2.clear();
    list2.shrink_to_fit();
    return newRDD.parseMaps(maps);
}

// Tow Maps so we know wich type to use 

template <typename OLDTYPE>
std::map<const std::string, const std::vector<std::string> (*)(std::vector<OLDTYPE> &, std::queue<Maps> &, const std::string)> mapCastRDD = {
    {"INT", parseMapsCastRDD<int, OLDTYPE>},
    {"LONG", parseMapsCastRDD<long int, OLDTYPE>},
    {"FLOAT", parseMapsCastRDD<float, OLDTYPE>},
    {"DOUBLE", parseMapsCastRDD<double, OLDTYPE>},
    {"LONG_DOUBLE", parseMapsCastRDD<long double, OLDTYPE>},
    {"STRING", parseMapsCastRDD<std::string, OLDTYPE>},
};

template <typename OLDTYPE1, typename OLDTYPE2>
std::map<const std::string, const std::vector<std::string> (*)(std::vector<OLDTYPE1> &, std::vector<OLDTYPE2> &, std::queue<Maps> &, const std::string)> mapCastRDDTuple = {
    {"INT INT", parseMapsCastRDDPair<int, int, OLDTYPE1, OLDTYPE2>},
    {"INT LONG", parseMapsCastRDDPair<int, long int, OLDTYPE1, OLDTYPE2>},
    {"INT FLOAT", parseMapsCastRDDPair<int, float, OLDTYPE1, OLDTYPE2>},
    {"INT DOUBLE", parseMapsCastRDDPair<int, double, OLDTYPE1, OLDTYPE2>},
    {"INT LONG_DOUBLE", parseMapsCastRDDPair<int, long double, OLDTYPE1, OLDTYPE2>},
    {"INT STRING", parseMapsCastRDDPair<int, std::string, OLDTYPE1, OLDTYPE2>},
    {"LONG INT", parseMapsCastRDDPair<long int, int, OLDTYPE1, OLDTYPE2>},
    {"LONG LONG", parseMapsCastRDDPair<long int, long int, OLDTYPE1, OLDTYPE2>},
    {"LONG FLOAT", parseMapsCastRDDPair<long int, float, OLDTYPE1, OLDTYPE2>},
    {"LONG DOUBLE", parseMapsCastRDDPair<long int, double, OLDTYPE1, OLDTYPE2>},
    {"LONG LONG_DOUBLE", parseMapsCastRDDPair<long int, long double, OLDTYPE1, OLDTYPE2>},
    {"LONG STRING", parseMapsCastRDDPair<long int, std::string, OLDTYPE1, OLDTYPE2>},
    {"FLOAT INT", parseMapsCastRDDPair<float, int, OLDTYPE1, OLDTYPE2>},
    {"FLOAT LONG", parseMapsCastRDDPair<float, long int, OLDTYPE1, OLDTYPE2>},
    {"FLOAT FLOAT", parseMapsCastRDDPair<float, float, OLDTYPE1, OLDTYPE2>},
    {"FLOAT DOUBLE", parseMapsCastRDDPair<float, double, OLDTYPE1, OLDTYPE2>},
    {"FLOAT LONG_DOUBLE", parseMapsCastRDDPair<float, long double, OLDTYPE1, OLDTYPE2>},
    {"FLOAT STRING", parseMapsCastRDDPair<float, std::string, OLDTYPE1, OLDTYPE2>},
    {"DOUBLE INT", parseMapsCastRDDPair<double, int, OLDTYPE1, OLDTYPE2>},
    {"DOUBLE LONG", parseMapsCastRDDPair<double, long int, OLDTYPE1, OLDTYPE2>},
    {"DOUBLE FLOAT", parseMapsCastRDDPair<double, float, OLDTYPE1, OLDTYPE2>},
    {"DOUBLE DOUBLE", parseMapsCastRDDPair<double, double, OLDTYPE1, OLDTYPE2>},
    {"DOUBLE LONG_DOUBLE", parseMapsCastRDDPair<double, long double, OLDTYPE1, OLDTYPE2>},
    {"DOUBLE STRING", parseMapsCastRDDPair<double, std::string, OLDTYPE1, OLDTYPE2>},
    {"LONG_DOUBLE INT", parseMapsCastRDDPair<long double, int, OLDTYPE1, OLDTYPE2>},
    {"LONG_DOUBLE LONG", parseMapsCastRDDPair<long double, long int, OLDTYPE1, OLDTYPE2>},
    {"LONG_DOUBLE FLOAT", parseMapsCastRDDPair<long double, float, OLDTYPE1, OLDTYPE2>},
    {"LONG_DOUBLE DOUBLE", parseMapsCastRDDPair<long double, double, OLDTYPE1, OLDTYPE2>},
    {"LONG_DOUBLE LONG_DOUBLE", parseMapsCastRDDPair<long double, long double, OLDTYPE1, OLDTYPE2>},
    {"LONG_DOUBLE STRING", parseMapsCastRDDPair<long double, std::string, OLDTYPE1, OLDTYPE2>},
    {"STRING INT", parseMapsCastRDDPair<std::string, int, OLDTYPE1, OLDTYPE2>},
    {"STRING LONG", parseMapsCastRDDPair<std::string, long int, OLDTYPE1, OLDTYPE2>},
    {"STRING FLOAT", parseMapsCastRDDPair<std::string, float, OLDTYPE1, OLDTYPE2>},
    {"STRING DOUBLE", parseMapsCastRDDPair<std::string, double, OLDTYPE1, OLDTYPE2>},
    {"STRING LONG_DOUBLE", parseMapsCastRDDPair<std::string, long double, OLDTYPE1, OLDTYPE2>},
    {"STRING STRING", parseMapsCastRDDPair<std::string, std::string, OLDTYPE1, OLDTYPE2>},
};
