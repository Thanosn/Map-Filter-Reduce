#pragma once

#include "RDD_Pair.h"
#include "Executor.h"

#include <iostream>
#include <vector>
#include <utility>
#include <queue>

template <typename T, typename U>
RDD<std::pair<T, U>>::RDD(const Vector_Pair_T &list) : keys(std::move(list)) {}

template <typename T, typename U>
RDD<std::pair<T, U>>::RDD(const std::vector<T> &list1, const std::vector<U> &list2)
{
    if (list1.size() == list2.size())
        this->keys = std::make_pair(std::move(list1), std::move(list2));
}

template <typename T, typename U>
template <typename T1, typename std::enable_if_t<!std::is_same_v<T1, std::string>> *> //Weird cpp things (cant use T so we use T1 = T)
const std::vector<std::string> RDD<std::pair<T, U>>::getFirstKeysToString() const
{
    return typeCastList<std::string, T>(std::get<0>(this->keys));
}

template <typename T, typename U>
template <typename T1, typename std::enable_if_t<std::is_same_v<T1, std::string>> *>
const std::vector<std::string> RDD<std::pair<T, U>>::getFirstKeysToString() const
{
    return std::get<0>(this->keys);
}

template <typename T, typename U>
template <typename U1, typename std::enable_if_t<!std::is_same_v<U1, std::string>> *>
const std::vector<std::string> RDD<std::pair<T, U>>::getSecondKeysToString() const
{
    return typeCastList<std::string, U>(std::get<1>(this->keys));
}

template <typename T, typename U>
template <typename U1, typename std::enable_if_t<std::is_same_v<U1, std::string>> *>
const std::vector<std::string> RDD<std::pair<T, U>>::getSecondKeysToString() const
{
    return std::get<1>(this->keys);
}

template <typename T, typename U>
const std::vector<std::string> RDD<std::pair<T, U>>::getKeysToString() const
{

    const auto firstValues = getFirstKeysToString();
    const auto secondValues = getSecondKeysToString();

    std::vector<std::string> result;
    result.reserve(firstValues.size());
    for (size_t i = 0; i < firstValues.size(); i++)
    {
        result.push_back("" + firstValues[i] + " " + secondValues[i] + "");
    }
    return result;
}

template <typename T, typename U>
std::string RDD<std::pair<T, U>>::count() const
{
    return std::to_string(std::get<0>(this->keys).size());
}

template <typename T, typename U>
void RDD<std::pair<T, U>>::executeMap(const struct Maps &map, const struct Maps &map2)
{
    const std::vector<U> list1 = typeCastList<U, T>(std::get<0>(this->keys));
    const std::vector<T> list2 = typeCastList<T, U>(std::get<1>(this->keys));

    const std::vector<T> firstValue = Parser::map_executor<T>(map.function, map.resultedType, std::get<0>(this->keys), list2);
    const std::vector<U> secondValue = Parser::map_executor<U>(map2.function, map2.resultedType, list1, std::get<1>(this->keys));

    this->keys = std::make_pair(firstValue, secondValue);
}

template <typename T, typename U>
template <typename NEWTYPE, typename std::enable_if<std::is_same<T, NEWTYPE>::value &&
                                                    std::is_same<U, NEWTYPE>::value>::type *>
std::vector<NEWTYPE> RDD<std::pair<T, U>>::executeMapWithoutStoringAndCast(const struct Maps &map)
{
    const std::vector<T> firstValue = std::get<0>(this->keys);
    const std::vector<U> secondValue = std::get<1>(this->keys);

    const std::vector<NEWTYPE> result = Parser::map_executor(map.function, map.resultedType, firstValue, secondValue);

    return result;
}

template <typename T, typename U>
template <typename NEWTYPE, typename std::enable_if<!std::is_same<T, NEWTYPE>::value ||
                                                    !std::is_same<U, NEWTYPE>::value>::type *>
std::vector<NEWTYPE> RDD<std::pair<T, U>>::executeMapWithoutStoringAndCast(const struct Maps &map)
{
    const std::vector<NEWTYPE> list1 = typeCastList<NEWTYPE, T>(std::get<0>(this->keys));
    const std::vector<NEWTYPE> list2 = typeCastList<NEWTYPE, U>(std::get<1>(this->keys));

    const std::vector<NEWTYPE> result = Parser::map_executor(map.function, map.resultedType, list1, list2);

    return result;
}

template <typename T, typename U>
void RDD<std::pair<T, U>>::executeFilter(const struct Maps &map)
{
    const size_t size = std::get<0>(this->keys).size();

    std::vector<T> temp_rdd_vector1;
    temp_rdd_vector1.reserve(size);
    std::vector<U> temp_rdd_vector2;
    temp_rdd_vector2.reserve(size);

    for (size_t i = 0; i < size; i++)
    {
        T x1 = std::get<0>(this->keys)[i];
        U x2 = std::get<1>(this->keys)[i];
        bool y = Parser::pair_filter_executor(map.function, x1, x2);
        if (y)
        {
            temp_rdd_vector1.push_back(x1);
            temp_rdd_vector2.push_back(x2);
        }
    }

    this->keys = std::make_pair(std::move(temp_rdd_vector1), std::move(temp_rdd_vector2));
}

template <typename T, typename U>
void RDD<std::pair<T, U>>::executeReduceByKey(const struct Maps &map)
{
    this->keys = Parser::pair_reduce_by_key_executor(map.function, std::get<0>(this->keys), std::get<1>(this->keys));
}

template <typename T, typename U>
template <typename NEWTYPE>
const std::vector<std::string> RDD<std::pair<T, U>>::executeAndCastToType(RDD<std::pair<T, U>> *rdd, std::queue<Maps> &maps)
{
    const Maps map = maps.front();
    std::vector<NEWTYPE> values = rdd->executeMapWithoutStoringAndCast<NEWTYPE>(map);
    maps.pop();
    rdd->keys = {};
    return parseMapsCastRDD<NEWTYPE>(values, maps, map.resultedType);
}

template <typename T, typename U>
std::map<std::string, const std::vector<std::string> (*)(RDD<std::pair<T, U>> *, std::queue<Maps> &maps)> RDD<std::pair<T, U>>::mapCastPairToType{
    {"INT", executeAndCastToType<int>},
    {"LONG", executeAndCastToType<long int>},
    {"FLOAT", executeAndCastToType<float>},
    {"DOUBLE", executeAndCastToType<double>},
    {"LONG_DOUBLE", executeAndCastToType<long double>},
    {"STRING", executeAndCastToType<std::string>},
};

template <typename T, typename U>
const std::vector<std::string> RDD<std::pair<T, U>>::parseMaps(std::queue<Maps> &maps)
{
    while (!maps.empty())
    {
        const Maps map = maps.front();
        if (map.resultedType == "FILTER")
        {
            this->executeFilter(map);
        }
        else if (map.resultedType == "REDUCE_BY_KEY")
        {
            printf("REDUCE_BY_KEY\n");
            this->executeReduceByKey(map);
            return this->getKeysToString();
        }
        else if (map.resultedType == "COUNT")
        {
            printf("COUNT\n");
            std::vector<std::string> vect{ this->count() };
            return vect;
        }
        else if (map.resultedType == "" || map.resultedType.substr(0, map.resultedType.find(" ")) == "TUPLE")
        {
            const std::string tupleDelimeter = " ";
            std::string str = map.resultedType;
            const std::size_t pos = str.find(tupleDelimeter);
            str.erase(0, pos + tupleDelimeter.length());
            maps.pop();
            const Maps map2 = maps.front();
            this->executeMap(map, map2);
            if (str != this->Type)
            {
                maps.pop();
                return mapCastRDDTuple<T, U>[str](std::get<0>(this->keys), std::get<1>(this->keys), maps, str);
            }
        }
        else
        {
            return mapCastPairToType[map.resultedType](this, maps);
        }
        maps.pop();
    }
    return this->getKeysToString();
}