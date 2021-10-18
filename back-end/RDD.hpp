#pragma once

#include "RDD.h"
#include "Executor.h"

#include <iostream>
#include <sstream>
#include <tuple>
#include <queue>

template <typename T>
RDD<T>::RDD(const std::vector<T> &list)
    : keys(std::move(list))
{
}

template <typename T>
std::string RDD<T>::count() const
{
    return std::to_string(this->keys.size());
}

template <typename T>
template <typename T1, typename std::enable_if_t<!std::is_same_v<T1, std::string>> *>
const std::vector<std::string> RDD<T>::getKeysToString() const
{
    std::vector<std::string> result;
    result.reserve(keys.size());
    for (auto &key : keys)
    {
        result.push_back(std::to_string(key));
    }
    return result;
}

template <typename T>
template <typename T1, typename std::enable_if_t<std::is_same_v<T1, std::string>> *>
const std::vector<std::string> RDD<T>::getKeysToString() const
{
    return keys;
}

template <typename T>
void RDD<T>::executeMap(const struct Maps &map)
{
    this->keys = Parser::map_executor(map.function, map.resultedType, this->keys, this->keys);
}

template <typename T>
std::vector<T> RDD<T>::executeMapWithoutStoring(const struct Maps &map)
{
    return Parser::map_executor(map.function, map.resultedType, this->keys, this->keys);
}

template <typename T>
const std::vector<std::string> RDD<T>::parseMaps(std::queue<Maps> &maps)
{
    while (!maps.empty())
    {
        const Maps map = maps.front();

        if (map.resultedType == "REDUCE")
        {
            printf("Reduce\n");
            this->executeMap(map);
            return this->getKeysToString();
        }
        else if (map.resultedType == "COUNT")
        {
            printf("COUNT\n");
            std::vector<std::string> vect{this->count()};
            return vect;
        }
        else if (map.resultedType == "" || map.resultedType == this->Type || map.resultedType == "FILTER")
        { //Normal Map
            this->executeMap(map);
        }
        else if (map.resultedType.substr(0, map.resultedType.find(" ")) == "TUPLE")
        {
            auto firstValue = this->executeMapWithoutStoring(map);
            maps.pop();

            const Maps map2 = maps.front();
            auto secondValue = this->executeMapWithoutStoring(map2);
            maps.pop();

            const std::string tupleDelimeter = " ";
            std::string str = map.resultedType;

            const std::size_t pos = str.find(tupleDelimeter);
            str.erase(0, pos + tupleDelimeter.length());

            keys.clear();
            keys.shrink_to_fit();
            return mapCastRDDTuple<T, T>[str](firstValue, secondValue, maps, str);
        }
        else
        {
            return mapCastRDD<T>[map.resultedType](this->keys, maps, map.resultedType);
        }
        maps.pop();
    }
    return this->getKeysToString();
}
