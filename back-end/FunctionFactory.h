#pragma once

#include <dlfcn.h>
#include <fstream>
#include <iostream>
#include <vector>
#include <utility>
#include <string>
#include <sstream>

extern int my_worker_id;
class FunctionFactory
{
private:
    typedef std::string PRE_TYPE;
    typedef std::string FUNC_PATH;
    static std::vector<std::pair<PRE_TYPE, FUNC_PATH>> functions;
    static void *handle;
    static long int functionCounter;

public:
    //Return the amount of functions we have 
    static inline size_t getSize() { return functions.size(); }

    //Parse the resulted type and create the function signature
    static void createFunctions(const std::vector<std::string> &commands, const std::vector<std::string> &types, const std::string resultedType, const bool isFirstMap = true)
    {

        std::string my_token1 = "";
        std::string my_token2 = "";

        std::string my_acc = "";

        for (size_t i = 0; i < types.size(); i = i + 1)
        {
            if (types[i].find("TUPLE_ACCESS") != std::string::npos)
            {
                if (commands[i + 1] == "1")
                    my_token1 = commands[i - 1] + commands[i + 1];
                if (commands[i + 1] == "2")
                    my_token2 = commands[i - 1] + commands[i + 1];
            }
            else if (types[i].find("TOKEN") != std::string::npos)
            {
                if (my_token1 == "")
                    my_token1 = commands[i];
            }
            else if (types[i].find("ACC") != std::string::npos)
            {
                if (my_acc == "")
                    my_acc = commands[i];
            }
        }

        std::string expression_string;
        for (const auto &piece : commands)
            expression_string += piece;

        size_t index = 0;
        while (true)
        {
            /* Locate the substring to replace. */
            index = expression_string.find("._1", index);
            if (index == std::string::npos)
                break;

            /* Make the replacement. */
            expression_string.replace(index, 3, "1");

            /* Advance index forward so the next iteration doesn't pick it up as well. */
            index += 1;
        }
        //std::cout << "EXPRESSIONNN 1 " << expression_string << "\n";

        index = 0;
        while (true)
        {
            /* Locate the substring to replace. */
            index = expression_string.find("._2", index);
            if (index == std::string::npos)
                break;

            /* Make the replacement. */
            expression_string.replace(index, 3, "2");

            /* Advance index forward so the next iteration doesn't pick it up as well. */
            index += 1;
        }

        std::string newType = "";
        std::string mylib_source = "";
        if (resultedType == "COUNT")
        {
            mylib_source = "void my_func" + std::to_string(functionCounter) + "() {}";
        }
        else if (resultedType.find("TUPLE") != std::string::npos)
        {
            bool found = false;

            for (int i = functionCounter; i > 0; i--)
            {
                if ((std::get<0>(functions[i - 1]).find("FILTER") != std::string::npos))
                {
                    continue;
                }
                if ((std::get<0>(functions[i - 1]).find("TUPLE") != std::string::npos))
                {
                    if (i == functionCounter)
                        continue;
                    //break;
                }

                const std::string s = std::get<1>(functions[i - 1]);
                newType = s.substr(0, s.find(" "));
                found = true;
                break;
            }

            if (!found)
            {
                std::stringstream ss(resultedType);
                std::string tuple;
                std::string firstType;
                std::string secondType;

                ss >> tuple;
                ss >> firstType;
                ss >> secondType;

                newType = isFirstMap ? firstType : secondType;
            }
            if (newType == "STRING")
                mylib_source = (newType + " my_func" + std::to_string(functionCounter) + "(const " + newType + "& " + my_token1 + ",const  " + newType + "& " + my_token2 + ") { \n\treturn ( to_str( " + expression_string + " ) );\n}");
            else
                mylib_source = (newType + " my_func" + std::to_string(functionCounter) + "(const " + newType + "& " + my_token1 + ",const  " + newType + "& " + my_token2 + ") { \n\treturn ( " + expression_string + " );\n}");
        }
        else if (resultedType == "FILTER" || resultedType == "REDUCE" || resultedType == "REDUCE_BY_KEY")
        {
            for (int i = functionCounter; i > 0; i--)
            {
                if ((std::get<0>(functions[i - 1]).find("FILTER") != std::string::npos))
                {
                    continue;
                }
                if ((std::get<0>(functions[i - 1]).find("TUPLE") != std::string::npos))
                {
                    std::stringstream ss(std::get<0>(functions[i - 1]));
                    std::string tuple;
                    std::string firstType;
                    std::string secondType;

                    ss >> tuple;
                    ss >> firstType;
                    ss >> secondType;
                    if (resultedType == "FILTER")
                    {
                        mylib_source = "bool my_func" + std::to_string(functionCounter) + "(const " + firstType + "& " + my_token1 + ",const  " + secondType + "& " + my_token2 + ") { \n\treturn ( " + expression_string + " );\n}";
                    }
                    else if (resultedType == "REDUCE")
                    {
                        const std::string pair_type = "std::pair<" + firstType + "," + secondType + ">";
                        mylib_source = pair_type + " my_func" + std::to_string(functionCounter) + "(const " + firstType + "& " + my_token1 + ",const  " + secondType + "& " + my_token2 + ",const  " + pair_type + "& " + my_acc + " ) { \n\treturn ( " + expression_string + " );\n}";
                    }
                    else if (resultedType == "REDUCE_BY_KEY")
                    {
                        mylib_source = secondType + " my_func" + std::to_string(functionCounter) + "(const " + secondType + "& " + my_token1 + ",const  " + secondType + "& " + my_acc + " ) { \n\treturn ( " + expression_string + " );\n}";
                    }
                    break;
                }
                const std::string s = std::get<1>(functions[i - 1]);
                newType = s.substr(0, s.find(" "));
                if (resultedType == "FILTER")
                {
                    mylib_source = "bool my_func" + std::to_string(functionCounter) + "(const " + newType + "& " + my_token1 + ",const  " + newType + "& " + my_token2 + ") { \n\treturn ( " + expression_string + " );\n}";
                }
                else if (resultedType == "REDUCE")
                {
                    mylib_source = newType + " my_func" + std::to_string(functionCounter) + "(const " + newType + "& " + my_token1 + ",const  " + newType + "& " + my_token2 + ",const " + newType + "& " + my_acc + " ) { \n\treturn ( " + expression_string + " );\n}";
                }
                break;
            }
        }
        else
        {
            std::map<std::string, std::string> functionStrings = {
                {"INT", "int my_func" + std::to_string(functionCounter) + "(const int& " + my_token1 + ", const int& " + my_token2 + ") { \n\treturn ( " + expression_string + " );\n}"},
                {"LONG", "long int my_func" + std::to_string(functionCounter) + "(const long int& " + my_token1 + ", const long int& " + my_token2 + ") { \n\treturn ( " + expression_string + " );\n}"},
                {"FLOAT", "float my_func" + std::to_string(functionCounter) + "(const float& " + my_token1 + ", const float& " + my_token2 + ") { \n\treturn ( " + expression_string + " );\n}"},
                {"DOUBLE", "double my_func" + std::to_string(functionCounter) + "(const double& " + my_token1 + ", const double& " + my_token2 + ") { \n\treturn ( " + expression_string + " );\n}"},
                {"LONG_DOUBLE", "long_double my_func" + std::to_string(functionCounter) + "(const long double& " + my_token1 + ", const long double& " + my_token2 + ") { \n\treturn ( " + expression_string + " );\n}"},
                {"STRING", "STRING my_func" + std::to_string(functionCounter) + "(const STRING& " + my_token1 + ", const STRING& " + my_token2 + ") { \n\treturn ( to_str( " + expression_string + " ) );\n}"},
            };
            mylib_source = functionStrings[resultedType];
        }

        if (mylib_source != "")
        {
            functions.push_back(std::make_pair(resultedType, mylib_source));
            functionCounter++;
        }
    }

    //Write generated functions, in the file, compile the file and return a dl hande from it
    static void compileFunctions()
    {
        std::ofstream myfile;
        myfile.open("mylib" + std::to_string(my_worker_id) + ".cpp");
        myfile << "#include <string>\n";
        myfile << "#include <type_traits>\n";
        myfile << "\ntypedef long double long_double;\n";
        myfile << "typedef const int INT;\n";
        myfile << "typedef const long int LONG;\n";
        myfile << "typedef const float FLOAT;\n";
        myfile << "typedef const double DOUBLE;\n";
        myfile << "typedef long_double LONG_DOUBLE;\n";
        myfile << "typedef const std::string STRING;\n\n";
        myfile << "template <typename T, typename std::enable_if_t<std::is_arithmetic_v<T>> * = nullptr>\n";
        myfile << "inline STRING to_str(const T& x) { return std::to_string(x); }\n";
        myfile << "template <typename T, typename std::enable_if_t<!std::is_arithmetic_v<T>> * = nullptr>\n";
        myfile << "inline STRING to_str(const T& x) { return x; }\n";

        for (auto &func : functions)
        {
            myfile << ("\n\nextern \"C\" " + std::get<1>(func));
        }
        myfile.close();

        printf("Worker %i compiling libs\n", my_worker_id);
        const int i = system(("g++ -Ofast -std=c++17 -c -o mylib" + std::to_string(my_worker_id) + ".o mylib" + std::to_string(my_worker_id) + ".cpp").c_str());
        const int y = system(("gcc -Ofast -shared -o mylib" + std::to_string(my_worker_id) + ".so mylib" + std::to_string(my_worker_id) + ".o").c_str());
        if (i || y)
            std::cout << "Lib Comilation Failed" << std::endl;
        handle = dlopen(("./mylib" + std::to_string(my_worker_id) + ".so").c_str(), RTLD_LAZY);
    }

    //Return a pointer to a function and the type
    static std::pair<std::string, void *> getFunction()
    {
        const long int index = functions.size() - functionCounter--;
        char name[12];  //Supporrts up to 998 workers
        sprintf(name, "my_func%ld", index);
        void *f = dlsym(handle, name);
        return std::make_pair(std::get<0>(functions[index]), f);
    }

    //Close the linked lib and empty the function vector
    static void closeLib()
    {
        if (handle)
            dlclose(handle);
        functionCounter = 0;
        functions.resize(0);
    }

    //Return the type of the RDD at the end of the maps( Neccessary for the master to know what RDD to use in Reduce cases )
    static std::string lastType()
    {
        for (size_t i = functions.size(); i > 0; i--)
        {
            const auto string = std::get<0>(functions[i - 1]);
            if ((string != "FILTER" && string != "REDUCE" && string != "REDUCE_BY_KEY" && string != "COUNT"))
                return string;
        }
        return "";
    }
};

long int FunctionFactory::functionCounter = 0;
std::vector<std::pair<std::string, std::string>> FunctionFactory::functions = {};
void *FunctionFactory::handle = nullptr;