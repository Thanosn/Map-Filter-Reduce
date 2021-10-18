#include "mfr.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include "FunctionFactory.h"
#include "RDD.hpp"
#include "RDD_Pair.hpp"
#include "TypeCastLists.h"
#include "Executor.h"
#include <vector>
#include <tuple>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using mfr::command;
using mfr::commands;
using mfr::dummy;
using mfr::mfr_command;
using mfr::par_command;
using mfr::result;

std::vector<std::string> temp_vec;
std::string rdd_type;
extern int my_worker_id;
class MfrServiceImpl final : public mfr_command::Service
{
  Status simple_handshake_command(ServerContext *context, const dummy *comm,
                                  mfr::result *fs) override
  {
    my_worker_id = comm->k();
    fs->add_list("OK");

    return Status::OK;
  }

  Status parallelize_command(ServerContext *context, const par_command *comm,
                             mfr::result *fs) override
  {
    printf("STARTED PARALLELIZING\n");
    temp_vec.clear();
    if (comm->isfile() == false)
    {
      temp_vec.reserve(comm->list_size());
      for (int i = 0; i < comm->list_size(); i++)
      {
        temp_vec.push_back(comm->list(i));
      }
    }
    else
    {
      size_t first_byte;
      size_t last_byte;

      std::ifstream myfile;
      if (comm->list_size() == 3)
      {
        first_byte = strtoll(comm->list(0).c_str(), NULL, 10);
        last_byte = strtoll(comm->list(1).c_str(), NULL, 10);
        myfile = std::ifstream(comm->list(2));
      }
      else
      {
        struct stat64 st;

        myfile = std::ifstream(comm->list(0));
        first_byte = 0;
        stat64(comm->list(0).c_str(), &st);
        last_byte = st.st_size;
      }

      std::string line;
      if (myfile.is_open())
      {
        myfile.seekg(first_byte);
        if (first_byte != 0)
        {
          myfile.seekg(first_byte - 1);
          char c;
          myfile.get(c);
          if (c != '\n')
          {
            std::string line;
            getline(myfile, line);
          }
        }

        while (getline(myfile, line))
        {
          temp_vec.push_back(line);

          size_t curr_off = myfile.tellg();
          if (curr_off >= last_byte)
            break;
        }
        myfile.close();
      }
    }
    rdd_type = comm->type();
    fs->add_list("OK");

    printf("Parrallelize Finished\n");

    return Status::OK;
  }

  Status map_command(ServerContext *context, const commands *comm,
                     mfr::result *fs) override
  {
    printf("Creating RDD\n");
    RDD<std::string> mRdd = RDDParallelize::parallelize<std::string>(temp_vec);
    mRdd.Type = "STRING";
    temp_vec.clear();
    temp_vec.shrink_to_fit();
    FunctionFactory::createFunctions({"x"}, {"TOKEN"}, rdd_type);

    printf("Reading Maps\n");
    int tuplecount = 0;

    for (int i = 0; i < comm->command_list_size(); i++)
    {
      std::vector<std::string> codes;
      std::vector<std::string> types;

      for (int j = 0; j < comm->command_list(i).code_list_size(); j++)
      {
        codes.push_back(comm->command_list(i).code_list(j));
        types.push_back(comm->command_list(i).type_list(j));
      }
      FunctionFactory::createFunctions(codes, types, comm->command_list(i).resulttype());

      if (comm->command_list(i).istuple())
      {
        std::vector<std::string> codesTuple;
        std::vector<std::string> typesTuple;
        for (int j = 0; j < comm->command_list_tuple(tuplecount).code_list_size(); j++)
        {
          codesTuple.push_back(comm->command_list_tuple(tuplecount).code_list(j));
          typesTuple.push_back(comm->command_list_tuple(tuplecount).type_list(j));
        }
        FunctionFactory::createFunctions(codesTuple, typesTuple, comm->command_list_tuple(tuplecount).resulttype(), true);
        tuplecount++;
      }
    }
    FunctionFactory::compileFunctions();

    std::queue<Maps> maps2;
    for (int i = 0; i < FunctionFactory::getSize(); i++)
    {
      auto far = FunctionFactory::getFunction();
      maps2.push({std::get<0>(far), std::get<1>(far)});
    }

    printf("Executing Maps\n");
    auto results = mRdd.parseMaps(maps2);
    printf("Executed Maps\n");

    fs->add_list(FunctionFactory::lastType());

    if (comm->isfile() == false)
    {
      for (auto &a : results)
      {
        fs->add_list(a);
      }
    }
    else
    {
      std::ofstream outfile;

      //outfile.open(comm->list_size() == 3 ? comm->list(2) + "_result" : comm->list(0) + "result", std::ios_base::app); // append instead of overwrite
      outfile.open("test_results" + std::to_string(my_worker_id) + ".dat");
      for (auto &a : results)
      {
        outfile << a << "\n";
      }
      outfile.close();
    }

    FunctionFactory::closeLib();
    return Status::OK;
  }
};

void RunServer(std::string port)
{
  std::string server_address("0.0.0.0:" + port);
  MfrServiceImpl service;

  grpc::EnableDefaultHealthCheckService(true);
  grpc::reflection::InitProtoReflectionServerBuilderPlugin();
  ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(&service);
  // Finally assemble the server.
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
}

int main(int argc, char **argv)
{
  RunServer(argv[1]);

  return 0;
}
