#pragma once

#include "RDD.hpp"

#include <iostream>
#include <vector>
#include <utility>
#include <queue>
#include <map>

template <typename T, typename U>
class RDD<std::pair<T, U>>
{
public:
    typedef typename std::pair<std::vector<T>, std::vector<U>> Vector_Pair_T; 
    std::string Type;

private:
    Vector_Pair_T keys;

    //Return the first vector of keys in string form
    template <typename T1 = T, typename std::enable_if_t<!std::is_same_v<T1, std::string>> * = nullptr> //Weird cpp things (cant use T so we use T1 = T)
    const std::vector<std::string> getFirstKeysToString() const;

    template <typename T1 = T, typename std::enable_if_t<std::is_same_v<T1, std::string>> * = nullptr>
    const std::vector<std::string> getFirstKeysToString() const;

    //Return the second vector of keys in string form    
    template <typename U1 = U, typename std::enable_if_t<!std::is_same_v<U1, std::string>> * = nullptr> //Weird cpp things (cant use U so we use U1 = U)
    const std::vector<std::string> getSecondKeysToString() const;

    template <typename U1 = U, typename std::enable_if_t<std::is_same_v<U1, std::string>> * = nullptr>
    const std::vector<std::string> getSecondKeysToString() const;

public:
    //Create an RDD<pair> from a Vector<pair>
    RDD(const Vector_Pair_T &list);

    //Create an RDD<pair> from 2 Vectors
    RDD(const std::vector<T> &list1, const std::vector<U> &list2);

    // RDD(const RDD<T> &rdd);
    // RDD(std::string filepath);

    //Return all keys in string form
    const std::vector<std::string> getKeysToString() const;

    //Return the amount of ellements in the RDD
    std::string count() const;

    //Execute a map command
    void executeMap(const struct Maps &map, const struct Maps &map2);

    template <typename NEWTYPE, typename std::enable_if<std::is_same<T, NEWTYPE>::value &&
                                                        std::is_same<U, NEWTYPE>::value>::type * = nullptr>
    std::vector<NEWTYPE> executeMapWithoutStoringAndCast(const struct Maps &map); //NEWTYPE = T = U

    template <typename NEWTYPE, typename std::enable_if<!std::is_same<T, NEWTYPE>::value ||
                                                        !std::is_same<U, NEWTYPE>::value>::type * = nullptr>
    std::vector<NEWTYPE> executeMapWithoutStoringAndCast(const struct Maps &map); // NEWTYPE != U or/and NEWTYPE != T

    //Execute a filter command
    void executeFilter(const struct Maps &map);
    //Execute a ReduceByKey command
    void executeReduceByKey(const struct Maps &map);

    template <typename NEWTYPE>
    static const std::vector<std::string> executeAndCastToType(RDD<std::pair<T, U>> *, std::queue<Maps> &maps);

    static std::map<std::string, const std::vector<std::string> (*)(RDD<std::pair<T, U>> *, std::queue<Maps> &maps)> mapCastPairToType;

    //Parse and execute all maps and return the results
    const std::vector<std::string> parseMaps(std::queue<Maps> &maps);
};