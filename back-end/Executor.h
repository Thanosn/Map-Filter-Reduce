#pragma once

#include <string>
#include <algorithm>
#include <vector>
#include <dlfcn.h>

int my_worker_id=0;

class Parser
{
public:

    //Prepare and execute the function for each element of the rdd 
    template <typename T>
    static std::vector<T> map_executor(void *functionPointer, const std::string resultedType, const std::vector<T> &keys1, const std::vector<T> &keys2)
    {
        const size_t size = keys1.size();

        std::vector<T> temp_rdd_vector;
        temp_rdd_vector.reserve(size);

        if (resultedType == "FILTER") //Only for normal RDDs
        {
            typedef bool (*myfunc_t)(const T&, const T&);
            myfunc_t myfunc = (myfunc_t)functionPointer;
            for (size_t i = 0; i < size; i++)
            {
                const T x1 = keys1[i];
                const T x2 = keys2[i];
                const bool y = (*myfunc)(x1, x2);
                if (y)
                    temp_rdd_vector.push_back(x1);
            }
        }
        else if (resultedType == "REDUCE") //Only for normal RDDs
        {
            typedef const T (*myfunc_t)(const T&, const T&, const T&);
            myfunc_t myfunc = (myfunc_t)functionPointer;
            T y = (T)(0);
            for (size_t i = 0; i < size; i++)
            {
                const T x1 = keys1[i];
                const T x2 = keys2[i];
                y = (*myfunc)(x1, x2, y);
            }
            temp_rdd_vector.push_back(y);
        }
        else
        {
            typedef const T (*myfunc_t)(const T&, const T&);
            myfunc_t myfunc = (myfunc_t)functionPointer;
            for (size_t i = 0; i < size; i++)
            {
                const T x1 = keys1[i];
                const T x2 = keys2[i];  //x2 in nor pair RDDs will be ignored by the function so we dont care what key2 have
                const T y = (*myfunc)(x1, x2);
                temp_rdd_vector.push_back(y);
            }
        }

        return temp_rdd_vector;
    }

    //Special function for pairs and filters
    template <typename T, typename U>
    static bool pair_filter_executor(void *functionPointer, const T &key1, const U &key2)
    {
        typedef const bool (*myfunc_t)(const T&, const U&);
        myfunc_t myfunc = (myfunc_t)functionPointer;

        const T x1 = key1;
        const U x2 = key2;
        const bool y = (*myfunc)(x1, x2);

        return y;
    }

    //Special function for pairs and reduce by key
    template <typename T, typename U>
    static std::pair<std::vector<T>, std::vector<U>> pair_reduce_by_key_executor(void *functionPointer, const std::vector<T> &keys1, const std::vector<U> &keys2)
    {
        const size_t size = keys1.size();

        std::vector<T> temp_key_vector;
        std::vector<U> temp_value_vector;

        temp_key_vector.reserve(size);
        temp_value_vector.reserve(size);

        typedef const U (*myfunc_t)(const U&, const U&);
        myfunc_t myfunc = (myfunc_t)functionPointer;

        U y;
        for (size_t i = 0; i < size; i++)
        {

            const T x1 = keys1[i];
            const U x2 = keys2[i];
            auto it = std::find(temp_key_vector.begin(), temp_key_vector.end(), x1);
            if (it != temp_key_vector.end())
            {
                int index = it - temp_key_vector.begin();
                y = temp_value_vector[index];
                y = (*myfunc)(x2, y);
                temp_value_vector[index] = y;
            }
            else
            {
                y = (*myfunc)(x2, (U)(0));
                temp_key_vector.push_back(x1);
                temp_value_vector.push_back(y);
            }
        }
        temp_key_vector.shrink_to_fit();
        temp_value_vector.shrink_to_fit();

        return std::make_pair(std::move(temp_key_vector), std::move(temp_value_vector));
    }
};
