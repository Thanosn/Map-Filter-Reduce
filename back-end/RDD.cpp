#include "RDD.hpp"
#include "RDD_Pair.hpp"

#include "FunctionFactory.h"
#include "TypeCastLists.h"
#include "Executor.h"

#include <iostream>

#include <vector>
#include <string>
#include <queue>

/*
int main()
{
    std::cout << "Starting Factory Tests beep boop" << std::endl;

    using T = std::string;
    std::vector<T> d = {"sad", "asd", "aw"};

    std::vector<double> g = {1.2, 2.5, 3.005};

    std::vector<float> f = {1.2f, 25.5f, 56.54f};
    const std::vector<std::string> e = {
        "2.0",
        "5",
        "6.005",
        "2.0",
        "5",
        "6.005",
        "2.0",
        "5",
        "6.005",
        "2.0",
        "5",
        "6.005",
        "2.0",
        "5",
        "6.005",
        "2.0",
        "5",
        "6.005",
        "2.0",
        "5",
        "6.005",
        "2.0",
        "5",
        "6.005",
        "2.0",
        "5",
        "6.005",
        "2.0",
        "5",
        "6.005",
        "2.0",
        "5",
        "6.005",
        "2.0",
        "5",
        "6.005","2.0",
        "5",
        "6.005",
        "2.0",
        "5",
        "6.005",
        "2.0",
        "5",
        "6.005",
        "2.0",
        "5",
        "6.005",
        "2.0",
        "5",
        "6.005",
        "2.0",
        "5",
        "6.005",
    };

    //using R = std::pair<double, float>;

    // RDD<R> mRdd5 = RDDParallelize::parallelize(g, f);
    RDD<std::string> mRdd6 = RDDParallelize::parallelize<std::string, std::string>(e);
    mRdd6.Type = "STRING";

    FunctionFactory::createFunctions({"x"}, {"TOKEN"}, "INT");
    

    FunctionFactory::createFunctions({"X"}, {"TOKEN"}, "TUPLE INT INT");
    FunctionFactory::createFunctions({"1"}, {"NUM"}, "TUPLE INT INT", false);
    FunctionFactory::createFunctions({"y", "+", "x"}, {"ACC","OP","TOKEN"}, "REDUCE_BY_KEY");

    FunctionFactory::compileFunctions();

    std::queue<Maps> maps2;
    for (size_t i = 0; i < FunctionFactory::getSize(); i++)
    {
        auto far = FunctionFactory::getFunction();
        maps2.push({std::get<0>(far), std::get<1>(far)});
    }

    auto tmp = mRdd6.parseMaps(maps2);
    for (auto &a : tmp)
    {
        std::cout << a << " ";
        std::cout << std::endl;
    }

    FunctionFactory::closeLib();
}
*/