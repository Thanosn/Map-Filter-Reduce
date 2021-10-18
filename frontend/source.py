import MFRlib
import inspect


def main():
    #rdd = MFRlib.parallelize("data-file-num.dat","LONG",0)
    rdd = MFRlib.parallelize(["as","das","ret","as","das","ret"],"STRING")

    #rdd2 = rdd.map("x => (x+10)","LONG")
    #rdd3 = rdd2.filter("x =>  ( x > 15.0 )","")
    rdd4 = rdd.map("x => (x,1)","TUPLE STRING INT")
    #print(rdd3.reduce("(x,y) => y+1", "(x,y) => y+x"))
    print(rdd4.reduce_by_key("(x,y) => y+x", "(x,y) => y+x"))
    #print(rdd4.count())



if __name__ == "__main__":
    main()
