import re
import enum
import sys

class State(enum.Enum):
    VAR = 1
    DIGIT = 2
    OP = 3
    INIT = 4
    BOOL_OP = 5

class Type(enum.Enum):
    TOKEN = 1
    NUM = 3
    OP = 4
    OPEN_PARENTHESIS = 5
    CLOSE_PARENTHESIS = 6
    COMMA = 7
    BOOL_OP = 8
    TUPLE_ACCESS = 9
    TELEIA = 10
    LOGICOP = 11

def analyzeMapArgs(funcString):
    character = re.compile(r"[a-zA-Z]+")
    digit = re.compile(r"[0-9]+")
    op = re.compile(r"\+|\*|\/|\-")
    parenthesis = re.compile(r"\(|\)")
    comma = re.compile(r"\,")
    boolean_ops = re.compile(r"\<|\>|\=")
    lhs,rhs = re.split(r"\=\>",funcString.replace(" ",""))

    lexime = ""
    state = State.INIT
    lexime_list = []
    type_list = []
    parenthesis_count = 0
    found_teleia = 0
    found_and = 0
    found_or = 0
    for char in rhs:
        if(char == "&" and found_and == 0):
            lexime_list.append(lexime)
            if(state == State.VAR):
                type_list.append(Type.TOKEN.name)
            elif(state == State.DIGIT):
                type_list.append(Type.NUM.name)
            elif(state == State.OP or state == State.BOOL_OP):
                raise Exception("Op and LOGICOP")

            lexime = "&"
            found_and = 1
            state = State.INIT
            found_teleia = 0
            continue


        elif(char == "&" and found_and == 1):
            lexime += "&"
            found_and = 0
            lexime_list.append(lexime)
            type_list.append(Type.LOGICOP.name)
            state = State.INIT
            lexime = ""

        if(char == "|" and found_or == 0):
            lexime_list.append(lexime)
            if(char == "&" and found_and == 0):
                lexime_list.append(lexime)
            if(state == State.VAR):
                type_list.append(Type.TOKEN.name)
            elif(state == State.DIGIT):
                type_list.append(Type.NUM.name)
            elif(state == State.OP or state == State.BOOL_OP):
                raise Exception("Op and LOGICOP")

            lexime = "|"
            found_or = 1
            continue

        elif(char == "|" and found_or == 1):
            lexime += "|"
            found_or = 0
            lexime_list.append(lexime)
            type_list.append(Type.LOGICOP.name)
            state = State.INIT
            lexime = ""
        #Found Stop
        if(char == "."):
            if(state == State.VAR):
                #found access point of tuple
                lexime_list.append(lexime)
                type_list.append(Type.TOKEN.name)
            elif(state == State.DIGIT):
                lexime_list.append(lexime)
                lexime_list.append(".")
                type_list.append(Type.NUM.name)
                type_list.append(Type.TELEIA.name)
            else:
                raise Exception("Found . but not in var(access point) or num(float)")
            lexime = ""
            state = State.INIT
            found_teleia = 1
            continue
        if(char == "_"):
            if(found_teleia == 1):
                type_list.append(Type.TUPLE_ACCESS.name)
                lexime_list.append("._")
                state = State.INIT
                found_teleia = 0
            elif(state == State.VAR):
                lexime +=char
                found_teleia = 0
            else:
                raise Exception("Found _ not in var or access point")
        elif(found_teleia == 1 and type_list[len(type_list) -1] == "TOKEN"):
            raise Exception("var . and sth else after")

        #boolean op found
        if(boolean_ops.match(char) and state != State.BOOL_OP):
            if(state == State.VAR):
                lexime_list.append(lexime)
                type_list.append(Type.TOKEN.name)
            elif(state == State.DIGIT):
                lexime_list.append(lexime)
                type_list.append(Type.NUM.name)
            elif(state == State.OP):
                raise Exception("Logic error: OP before boolean expression")

            lexime = char
            state = State.BOOL_OP
            #type_list.append(Type.BOOL_OP);
            #!important look ahaid char for <= >= ==
        elif(boolean_ops.match(char) and state == State.BOOL_OP):
            lexime += char


        #comma found
        if(comma.match(char)):
            if(state == State.VAR):
                lexime_list.append(lexime)
                type_list.append(Type.TOKEN.name)
            elif(state == State.DIGIT):
                lexime_list.append(lexime)
                type_list.append(Type.NUM.name)
            elif(state == State.OP):
                raise Exception("Logic error : Op before comma")
            elif(state == State.BOOL_OP):
                raise Exception("COMMA AFTER LOGIC OP")

            lexime = ""
            state = State.INIT
            lexime_list.append(char)
            type_list.append(Type.COMMA.name)

        #parenthesi found
        elif(parenthesis.match(char)):
            if(state == State.VAR):
                lexime_list.append(lexime)
                type_list.append(Type.TOKEN.name)
            elif(state == State.DIGIT):
                lexime_list.append(lexime)
                type_list.append(Type.NUM.name)
            elif(state == State.OP):
                lexime_list.append(lexime)
                type_list.append(Type.OP.name)
            elif(state == State.BOOL_OP):
                lexime_list.append(lexime)
                type_list.append(Type.BOOL_OP.name)

            lexime = ""
            state = State.INIT
            lexime_list.append(char)
            if(char == '('):
                parenthesis_count += 1
                type_list.append(Type.OPEN_PARENTHESIS.name)
            else:
                if(parenthesis_count == 0):
                    raise Exception("Wrong parenthesis!")
                parenthesis_count -=1
                type_list.append(Type.CLOSE_PARENTHESIS.name)

        #character found
        elif(character.match(char) and state == State.VAR):
            lexime += char
        elif(character.match(char) and ( state == State.OP or state == State.DIGIT or state == State.INIT or state == State.BOOL_OP) ):
            if (lexime != ""):
                lexime_list.append(lexime)

            #also fill type_list
            if(state == State.OP):
                type_list.append(Type.OP.name)
            elif(state == State.DIGIT):
                type_list.append(Type.NUM.name)
            elif(state == State.BOOL_OP):
                type_list.append(Type.BOOL_OP.name)

            state = State.VAR
            lexime = char

        #Digit found
        elif(digit.match(char) and state == State.INIT):
            state = State.DIGIT
            lexime = char
        elif(digit.match(char) and ( state == State.DIGIT or state == State.VAR) ):
            lexime += char
        elif(digit.match(char) and state == State.OP ):
            lexime_list.append(lexime)
            type_list.append(Type.OP.name)
            lexime = char
            state = state.DIGIT
        elif(digit.match(char) and state == State.BOOL_OP):
            lexime_list.append(lexime)
            type_list.append(Type.BOOL_OP.name)
            lexime = char
            state = state.DIGIT
        #Op found
        elif(op.match(char) and state == State.INIT and len(type_list) == 0 ) :
            sys.exit(1)
            
        elif(op.match(char) and ( state == State.VAR or state == State.DIGIT or state == State.INIT) ):
            lexime_list.append(lexime)

            #also fill type_lst
            if(state == State.VAR):
                type_list.append(Type.TOKEN.name)
            elif(state == State.DIGIT):
                type_list.append(Type.NUM.name)
            elif(state == State.BOOL_OP):
                raise Exception("Op Before Bool OP")

            lexime = char
            state = State.OP
        elif(op.match(char) and state == State.OP):
            lexime +=char

    if(parenthesis_count != 0):
        raise Exception("Wrong parenthesis!")
    if(lexime != ""):
        lexime_list.append(lexime)

    if(state == State.VAR):
        type_list.append(Type.TOKEN.name)
    elif(state == State.OP):
        type_list.append(Type.OP.name)
    elif(state == State.DIGIT):
        type_list.append(Type.NUM.name)

    #(x,y)
    if( "," in lhs ) :
        token,acc = re.split(r"\,",lhs.replace(" ",""))
        acc = acc.replace(')','')
        count = 0
        for elem in lexime_list:
            if(elem == acc):
                type_list[count] = "ACC"
            count += 1

    return lexime_list,type_list
