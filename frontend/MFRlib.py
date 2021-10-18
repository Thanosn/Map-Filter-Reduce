from typing import List
import Parser as par
import executionGraph

execg = executionGraph.executionGraph()
worker1 = executionGraph.worker()
execg.addworker(worker1)

class RDD:
    id = 0

    def __init__(self, input, id):
        self.list = input
        self.paramList = []
        self.typeList = []
        self.result = []
        self.tupleTypeList = []
        self.tupleParamList = []
        self.rddid = RDD.id
        RDD.id += 1
        self.associated = id
        self.isTuple = 0
        self.typeoftransformation = ""
        self.isFile = False
        self.type = ""
        self.no_of_partitions = 0

    def tupleExist(self,typeList_tmp):
        foundparenthesis = 0
        for elem in typeList_tmp:
            if elem == "OPEN_PARENTHESIS":
                foundparenthesis += 1
            if elem == "CLOSE_PARENTHESIS":
                foundparenthesis -= 1
            if elem == "COMMA" and foundparenthesis > 0:
                return 1

        return 0

    def formFloats(self,typeList_tmp,paramList_tmp):
        i = 0
        for elem in typeList_tmp:
            if elem == "TELEIA":
                if typeList_tmp[i - 1] == "NUM" and typeList_tmp[i + 1] == "NUM":
                    paramList_tmp[i - 1] += paramList_tmp[i] + paramList_tmp[i + 1]
                    paramList_tmp.pop(i)
                    paramList_tmp.pop(i)
                    typeList_tmp[i - 1] = "FLOAT"
                    typeList_tmp.pop(i)
                    typeList_tmp.pop(i)
                else:
                    raise Exception("STH IS GOING WRONG ")
            i += 1


    def map(self, strfunc,type=""):
        tupleParamList = []
        tupleTypeList = []
        paramList_tmp, typeList_tmp = par.analyzeMapArgs(strfunc)
        isTuple = 0

        for elem in paramList_tmp:
            if elem == "":
                paramList_tmp.remove(elem)

        self.formFloats(typeList_tmp,paramList_tmp)
        if self.tupleExist(typeList_tmp):
            isTuple = 1
            # init
            #tupleParamList = []
            #tupleTypeList = []

            # remove tuple parenthesis
            typeList_tmp.pop(0)
            typeList_tmp.pop(len(typeList_tmp) - 1)
            paramList_tmp.pop(0)
            paramList_tmp.pop(len(paramList_tmp) - 1)

            # form typeLists
            tupleTypeList.append(typeList_tmp[: typeList_tmp.index("COMMA")])
            tupleTypeList.append(typeList_tmp[typeList_tmp.index("COMMA") + 1 :])

            # form paramList
            tupleParamList.append(paramList_tmp[: paramList_tmp.index(",")])
            tupleParamList.append(paramList_tmp[paramList_tmp.index(",") + 1 :])

        newrdd = RDD([], self.rddid)
        newrdd.paramList = paramList_tmp[:]
        newrdd.typeList = typeList_tmp[:]
        newrdd.isTuple = isTuple
        newrdd.typeoftransformation = "map"
        if type == "":
            newrdd.type = self.type
        else:
            newrdd.type = type
        # check this out for debugging
        newrdd.tupleTypeList = tupleTypeList[:]
        newrdd.tupleParamList = tupleParamList[:]

        for worker in execg.workersList:
            worker.addTransformation(newrdd)

        execg.printGraph()

        return newrdd

    
    def reduce(self, strfunc, strfunc2, type=""):
        paramList_tmp, typeList_tmp = par.analyzeMapArgs(strfunc)
        paramList_tmp2, typeList_tmp2 = par.analyzeMapArgs(strfunc2)

        for elem in paramList_tmp:
            if elem == "":
                paramList_tmp.remove(elem)

        for elem in paramList_tmp2:
            if elem == "":
                paramList_tmp2.remove(elem)

        newrdd = RDD([], self.rddid)
        newrdd.paramList = paramList_tmp[:]
        newrdd.typeList = typeList_tmp[:]
        newrdd.isTuple = 0
        newrdd.tupleTypeList = []
        newrdd.tupleParamList = []
        newrdd.typeoftransformation = "reduce"

        newrdd2 = RDD([], newrdd.rddid)
        newrdd2.paramList = paramList_tmp2[:]
        newrdd2.typeList = typeList_tmp2[:]
        newrdd2.isTuple = 0
        newrdd2.tupleTypeList = []
        newrdd2.tupleParamList = []
        newrdd2.typeoftransformation = "reduce"
        
        if type == "":
            newrdd.type = self.type
        else:
            newrdd.type = type

       
        if type == "":
            newrdd2.type = newrdd.type
        else:
            newrdd2.type = type

        for worker in execg.workersList:
            worker.addTransformation(newrdd)

        for worker in execg.workersList:
            result = worker.PerformAction(newrdd,newrdd2)
        return result


    def reduce_by_key(self, strfunc, strfunc2, type=""):
        paramList_tmp, typeList_tmp = par.analyzeMapArgs(strfunc)
        paramList_tmp2, typeList_tmp2 = par.analyzeMapArgs(strfunc2)

        for elem in paramList_tmp:
            if elem == "":
                paramList_tmp.remove(elem)

        for elem in paramList_tmp2:
            if elem == "":
                paramList_tmp2.remove(elem)

        newrdd = RDD([], self.rddid)
        newrdd.paramList = paramList_tmp[:]
        newrdd.typeList = typeList_tmp[:]
        newrdd.isTuple = 0
        newrdd.tupleTypeList = []
        newrdd.tupleParamList = []
        newrdd.typeoftransformation = "reduce_by_key"

        newrdd2 = RDD([], newrdd.rddid)
        newrdd2.paramList = paramList_tmp2[:]
        newrdd2.typeList = typeList_tmp2[:]
        newrdd2.isTuple = 0
        newrdd2.tupleTypeList = []
        newrdd2.tupleParamList = []
        newrdd2.typeoftransformation = "reduce_by_key"
        
        if type == "":
            newrdd.type = self.type
        else:
            newrdd.type = type

       
        if type == "":
            newrdd2.type = newrdd.type
        else:
            newrdd2.type = type

        for worker in execg.workersList:
            worker.addTransformation(newrdd)

        for worker in execg.workersList:
            result = worker.PerformAction(newrdd,newrdd2)
        return result

    def count(self):

        for worker in execg.workersList:
            result = worker.PerformAction(self,"COUNT")
        return result[0]

    def collect(self):
        result_list = []
        # collect op for rdd
        for worker in execg.workersList:
            result = worker.PerformAction(self,"COLLECT")
        
        if result[0] == "collect_file":
            i = 0
            file_name = "test_results" + str(i) + ".dat"
            f = open(file_name)
            while True:
                try:
                    line = f.readline()
                    if not line:
                        i +=1
                        file_name = "test_results" + str(i) + ".dat"
                        f = open(file_name)
                        continue
                    result_list.append(line.strip())
                except:
                    break
        
        else:
            result_list = result
        return result_list

    def take(self,num):
        result_list = []
        # take op for rdd
        for worker in execg.workersList:
           result = worker.PerformAction(self, "TAKE", num)
        #result
        if result[0] == "take_file":
            
            count = 0
            i = 0
            file_name = "test_results" + str(i) + ".dat"
            f = open(file_name)
            while count < num:
                try:
                    line = f.readline()
                    if not line:
                        i +=1
                        file_name = "test_results" + str(i) + ".dat"
                        f = open(file_name)
                        continue
                    result_list.append(line.strip())
                    count +=1
                except:
                    count +=1
        
        else:
            result_list = result
        return result_list




    def filter(self, strfunc,type=""):
        paramList_tmp, typeList_tmp = par.analyzeMapArgs(strfunc)
        for elem in paramList_tmp:
            if elem == "":
                paramList_tmp.remove(elem)
        newrdd = RDD([], self.rddid)
        newrdd.paramList = paramList_tmp[:]
        newrdd.typeList = typeList_tmp[:]
        newrdd.isTuple = 0
        newrdd.tupleTypeList = []
        newrdd.tupleParamList = []
        newrdd.typeoftransformation = "filter"
        if type == "":
            newrdd.type = self.type
        else:
            newrdd.type = type

        for worker in execg.workersList:
            worker.addTransformation(newrdd)

        execg.printGraph()
        return newrdd


def parallelize(input, rdd_type, no_of_partitions = 0):
    print(input)
    if type(input) == list:
        newrdd = RDD(input, -1)
        newrdd.type = rdd_type
        newrdd.no_of_partitions = no_of_partitions
        for worker in execg.workersList:
            worker.addTransformation(newrdd)
        return newrdd
    else:
        try:
            f = open(input, "r")
            newrdd = RDD(input, -1)
            newrdd.isFile = True
            newrdd.type = rdd_type
            newrdd.no_of_partitions = no_of_partitions
            for worker in execg.workersList:
                worker.addTransformation(newrdd)
            return newrdd
            
        except:
            raise Exception("File doesnt exist")
