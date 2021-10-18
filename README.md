# Hy543_MRF

For this project we used:
GCC >= 9: Becuase we use c++17 features 
Python3
CMake >= 3.13 
gRPC C++: https://grpc.io/docs/languages/cpp/quickstart/
gRPC Python: https://grpc.io/docs/languages/python/quickstart/
NFS if processes are not on the same machine

In order to compile project

mkdir build
cd build
cmake ..
make 


In order to run the backend:
    First create a file called hostfile and there add the ips to be used from the master and the workers(there is a localhost example in the back_end folder)
    To run the master use:
        ./master_backend  and the port
    To run each worker 
        ./test_backend and the port
    (The master will require at least on worker to be running when the front end starts sending messages)

In order to run the front-end:
    You should create a python source file(There is an example in the front-end/source.py file)
    In order to bring the changes from front-end folder to build folder run cmake ..
    Then just run: python3 source.py