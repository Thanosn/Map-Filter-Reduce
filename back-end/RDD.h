#pragma once

#include "TypeCastLists.h"

#include <iostream>
#include <vector>
#include <string>
#include <queue>
#include <map>

template <typename T>
class RDD
{
public:
    typedef std::vector<T> Vector_T; // Maybe use this

private:
    std::vector<T> keys;

public:
    std::string Type = "";

    //Create a RDD from Vector 
    RDD(const std::vector<T> &list);

    // RDD(const RDD<T> &rdd);
    // RDD(std::string filepath);

    //Return the amount of elements in the RDD
    std::string count() const;

    //Returna all elements in String form
    template <typename T1 = T, typename std::enable_if_t<!std::is_same_v<T1, std::string>> * = nullptr> //Weird cpp things (cant use T so we use T1 = T)
    const std::vector<std::string> getKeysToString() const;
    template <typename T1 = T, typename std::enable_if_t<std::is_same_v<T1, std::string>> * = nullptr>
    const std::vector<std::string> getKeysToString() const;

    //Execute a map
    void executeMap(const struct Maps &map);
    //Execute a map but dont store the results in the key
    std::vector<T> executeMapWithoutStoring(const struct Maps &map);
    //Parse and execute all maps and return the results in String form
    const std::vector<std::string> parseMaps(std::queue<Maps> &maps);
};

