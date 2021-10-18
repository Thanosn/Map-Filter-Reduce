import MFRlib as mfrlib
import enum
import grpc
import mfr_pb2
import mfr_pb2_grpc
master_info =""
initialized = 0


class TypeOfAction(enum.Enum):
    transformation = 0
    action = 1

class worker:
    id = 0
    def __init__(self):
        self.wid = worker.id
        worker.id += 1
        self.executionList = []

    def addTransformation(self,rdd):
        self.executionList.append( (rdd, TypeOfAction.transformation) )
    
    def PerformAction(self,rdd, action, num = 0):
        execute_action = ""
        global master_info
        print("Perform action")
        stack = []
        associatedID = rdd.rddid

        #search list backwards, when you find the rddid == associatedID update associated
        #until we find -1 (ending point)
        for i in range(len(self.executionList) -1 , -1, -1):
            if(self.executionList[i][0].rddid == associatedID):
                stack.append(self.executionList[i][0])
                associatedID = self.executionList[i][0].associated
            if associatedID == -1:
                break

        if(type(action) != str):
            stack.insert(0,action) #append the last rdd in the stack as map

        #Right order
        if type(action) == str:
            execute_action = action
        else:
            execute_action = "REDUCE"

        parallelize_list = []
        global initialized
        global master_info
        if (initialized == 0):
            initialized = 1
            f = open("hostfile", "r")
            master_info = f.readline()

        with grpc.insecure_channel(master_info) as channel:
            stub = mfr_pb2_grpc.mfr_commandStub(channel)

            first_command = stack.pop()
                #change elems to strings
            
            if(first_command.isFile):
                parallelize_list.append(first_command.list)
            else:
                for elem in first_command.list:
                    parallelize_list.append(str(elem))

            response = stub.parallelize_command(mfr_pb2.par_command(list = parallelize_list, isfile = first_command.isFile, type = first_command.type,no_of_partitions = first_command.no_of_partitions),  wait_for_ready=True)
            first_command.result = response.list

            Commands = mfr_pb2.commands()

            while len(stack) != 0:
                command = stack.pop()
                if(command.isTuple == 1):
                    if(command.typeoftransformation=="map"):
                        Commands.command_list.add(code_list = command.tupleParamList[0], type_list = command.tupleTypeList[0], resultType = command.type, isTuple = 1)
                        Commands.command_list_tuple.add(code_list = command.tupleParamList[1] , type_list = command.tupleTypeList[1], resultType = command.type, isTuple = 1)
                    elif(command.typeoftransformation=="reduce_by_key"):
                        Commands.command_list.add(code_list = command.tupleParamList[0], type_list = command.tupleTypeList[0], resultType = "REDUCE_BY_KEY", isTuple = 1)
                        Commands.command_list_tuple.add(code_list = command.tupleParamList[1] , type_list = command.tupleTypeList[1], resultType = "REDUCE_BY_KEY", isTuple = 1)
                else:
                    if(command.typeoftransformation=="filter"):
                        Commands.command_list.add(code_list = command.paramList, type_list = command.typeList , resultType = "FILTER" , isTuple = 0)
                    elif(command.typeoftransformation=="reduce"):
                        Commands.command_list.add(code_list = command.paramList, type_list = command.typeList , resultType = "REDUCE", isTuple = 0)
                        execute_action = "REDUCE"
                    elif(command.typeoftransformation=="reduce_by_key"):
                        Commands.command_list.add(code_list = command.paramList, type_list = command.typeList , resultType = "REDUCE_BY_KEY", isTuple = 0)
                        execute_action = "REDUCE_BY_KEY"
                    elif(command.typeoftransformation=="map"):
                        Commands.command_list.add(code_list = command.paramList, type_list = command.typeList , resultType = command.type, isTuple = 0)

            if(execute_action=="COUNT"):
                Commands.command_list.add(code_list = [], type_list = [] , resultType = execute_action, isTuple = 0)

            Commands.action_type = execute_action
            Commands.isFile = first_command.isFile
            Commands.take_arg = num
            response = stub.map_command(Commands)
            return response.list
        #last step
        

class executionGraph:
    def __init__(self):
        self.workersList = []


    def addworker(self,w):
        self.workersList.append(w)
        

    def printGraph(self):
        pass