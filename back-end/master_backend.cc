#include "mfr.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include "worker.hpp"
#include "FunctionFactory.h"
#include "RDD.hpp"
#include "RDD_Pair.hpp"
#include "TypeCastLists.h"
#include "Executor.h"
#include <fstream>
#include <sstream>
#include <thread>
#include <unistd.h>
#include <sys/stat.h>
#include <queue>
#include <string>

using grpc::Channel;
using grpc::ClientContext;
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

std::vector<Worker> worker_vec;
int no_of_partitions;

int handshake_worker(std::string IP, std::string port, int id)
{
  std::string target_str;
  target_str = IP + ":" + port;
  std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(
      target_str, grpc::InsecureChannelCredentials());

  std::unique_ptr<mfr_command::Stub> stub_ = mfr_command::NewStub(channel);
  dummy worker_id;
  worker_id.set_k(id);

  // Container for the data we expect from the server.
  result reply;

  // Context for the client. It could be used to convey extra information to
  // the server and/or tweak certain RPC behaviors.
  grpc::ClientContext context;

  std::chrono::time_point deadline = std::chrono::system_clock::now() +
                                     std::chrono::milliseconds(100);
  context.set_deadline(deadline);
  context.set_wait_for_ready(true);

  // The actual RPC.
  Status status = stub_->simple_handshake_command(&context, worker_id, &reply);

  // Act upon its status.
  if (status.ok())
  {
    return 1;
  }
  else
  {

    return 0;
  }
}

void handshake()
{
  std::ifstream myfile;
  myfile.open("hostfile");
  if (myfile.is_open())
  {
    std::string tp;
    getline(myfile, tp); //skippig first line which is the master
    int i = 0;
    while (getline(myfile, tp))
    { //read data from file object and put it into string.
      std::string IP;
      std::stringstream ssin(tp);
      getline(ssin, IP, ':');

      std::string port;
      getline(ssin, port);
      if (handshake_worker(IP, port, worker_vec.size()))
      {
        worker_vec.push_back(*(new Worker(IP, port, worker_vec.size())));
        printf("Handshaked worker at %s:%s\n", IP.c_str(), port.c_str());
      }
    }
    myfile.close(); //close the file object.
  }
}

class MfrServiceImpl final : public mfr_command::Service
{

  Status parallelize_command(ServerContext *context, const par_command *comm,
                             mfr::result *fs) override
  {
    worker_vec.clear();
    handshake();                    //checking for removed or added resources
    system("rm -rf test_results* mylib*"); //remove previous results
    int workload_per_worker;
    size_t file_size;
    if (comm->no_of_partitions() != 0 && comm->no_of_partitions() < worker_vec.size())
      no_of_partitions = comm->no_of_partitions();
    else
      no_of_partitions = worker_vec.size();
    //   = comm->list_size() / worker_vec.size(); //used only if the RDD is a local collection and NOT a file, otherwise uselesss
    grpc::CompletionQueue cq;

    Status status[no_of_partitions];
    result reply[no_of_partitions];
    std::thread thread_[no_of_partitions];
    grpc::ClientContext context_[no_of_partitions];
    par_command rdd_part[no_of_partitions];

    if (comm->isfile() == true)
    {
      struct stat64 st;
      stat64(comm->list(0).c_str(), &st);
      file_size = st.st_size;
      workload_per_worker = file_size / no_of_partitions;
    }
    else
      workload_per_worker = comm->list_size() / no_of_partitions;

    for (int i = 0; i < no_of_partitions; i++)
    {
      std::string target_str;
      target_str = worker_vec.at(i).getIP() + ":" + worker_vec.at(i).getport();
      std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(
          target_str, grpc::InsecureChannelCredentials());

      std::unique_ptr<mfr_command::Stub> stub_ = mfr_command::NewStub(channel);

      rdd_part[i].set_isfile(comm->isfile());
      if (i != no_of_partitions - 1) //each wokrer gets equal load except, possibly, for the last one
      {
        if (comm->isfile() == false)
        {
          for (int j = 0; j < workload_per_worker; j++)
          {
            rdd_part[i].add_list(comm->list((worker_vec.at(i).getid() * workload_per_worker) + j));
            rdd_part[i].set_type(comm->type());
          }
        }
        else
        {
          rdd_part[i].add_list(std::to_string(i * workload_per_worker));                       //first line of partition
          rdd_part[i].add_list(std::to_string(i * workload_per_worker + workload_per_worker)); //last line of partition
          rdd_part[i].add_list(comm->list(0));                                                 //the filepath
          rdd_part[i].set_type(comm->type());
        }
      }
      else
      { //the last worker takes his load + the remainder of the division
        if (comm->isfile() == false)
        {
          for (int j = 0; j < workload_per_worker + comm->list_size() % no_of_partitions; j++)
          {
            rdd_part[i].add_list(comm->list((worker_vec.at(i).getid() * workload_per_worker) + j));
            rdd_part[i].set_type(comm->type());
          }
        }
        else
        {
          rdd_part[i].add_list(std::to_string(i * workload_per_worker));                                                        //first line of partition
          rdd_part[i].add_list(std::to_string((i * workload_per_worker + workload_per_worker + file_size % no_of_partitions))); //last line of partition, maybebiggerforthe last worker
          rdd_part[i].add_list(comm->list(0));                                                                                  //the filepath
          rdd_part[i].set_type(comm->type());
        }
      }

      // Container for the data we expect from the server.

      std::unique_ptr<grpc::ClientAsyncResponseReader<result>> rpc(
          stub_->PrepareAsyncparallelize_command(&context_[i], rdd_part[i], &cq));
      rpc->StartCall();
      rpc->Finish(&reply[i], &status[i], (void *)1);
    }
    for (int i = 0; i < no_of_partitions; i++)
    {

      void *got_tag;
      bool ok = false;

      GPR_ASSERT(cq.Next(&got_tag, &ok));

      // Verify that the result from "cq" corresponds, by its tag, our previous
      // request.
      GPR_ASSERT(got_tag == (void *)1);
      // ... and that the request was completed successfully. Note that "ok"
      // corresponds solely to the request for updates introduced by Finish().
      GPR_ASSERT(ok);
    }
    fs->add_list("OK");

    printf("Parrallelize on Workers Finished\n");

    return Status::OK;
  }

  Status map_command(ServerContext *context, const commands *comm,
                     mfr::result *fs) override
  {

    grpc::CompletionQueue cq;
    Status status[no_of_partitions];
    result reply[no_of_partitions];
    std::thread thread_[no_of_partitions];
    grpc::ClientContext context_[no_of_partitions];

    std::unique_ptr<grpc::ClientAsyncResponseReader<result>> rpcs[no_of_partitions];
    for (int i = 0; i < no_of_partitions; i++)
    {
      std::string target_str;
      target_str = worker_vec.at(i).getIP() + ":" + worker_vec.at(i).getport();
      std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(
          target_str, grpc::InsecureChannelCredentials());

      std::unique_ptr<mfr_command::Stub> stub_ = mfr_command::NewStub(channel);
      // Container for the data we expect from the server.

      rpcs[i] = std::unique_ptr<grpc::ClientAsyncResponseReader<result>>(stub_->PrepareAsyncmap_command(&context_[i], *comm, &cq));
      rpcs[i]->StartCall();
      rpcs[i]->Finish(&reply[i], &status[i], (void *)1);
    }
    for (int i = 0; i < no_of_partitions; i++)
    {
      void *got_tag;
      bool ok = false;

      //  Status status = stub_->parallelize_command(&context, rdd_part, &reply);

      //while(1) ;

      GPR_ASSERT(cq.Next(&got_tag, &ok));

      // Verify that the result from "cq" corresponds, by its tag, our previous
      // request.
      GPR_ASSERT(got_tag == (void *)1);
      // ... and that the request was completed successfully. Note that "ok"
      // corresponds solely to the request for updates introduced by Finish().
      GPR_ASSERT(ok);
    }
    if ((comm->action_type() == "COLLECT" && comm->isfile() == false))
    {
      for (int i = 0; i < no_of_partitions; i++)
      {
        for (int j = 1; j < reply[i].list_size(); j++)
        {
          fs->add_list(reply[i].list(j));
        }
      }
    }
    else if ((comm->action_type() == "COLLECT" && comm->isfile() == true))
    {
      fs->add_list("collect_file");
    }
    else if ((comm->action_type() == "COUNT" && comm->isfile() == true))
    {
      long sum = 0;

      for (int i = 0; i < no_of_partitions; i++)
      {
        std::ifstream myfile;
        myfile.open("test_results" + std::to_string(i) + ".dat");
        std::string tp;
        getline(myfile, tp); //skippig first line which is the master
        sum = sum + strtoll(tp.c_str(), NULL, 10);
      }
      fs->add_list(std::to_string(sum));
    }
    else if ((comm->action_type() == "TAKE" && comm->isfile() == true))
    {

      fs->add_list("take_file");
    }
    else if ((comm->action_type() == "TAKE" && comm->isfile() == false))
    {
      int take_counter = 0;
      int break_flag = 0;
      for (int i = 0; i < no_of_partitions && break_flag == 0; i++)
      {
        for (int j = 1; j < reply[i].list_size(); j++)
        {
          fs->add_list(reply[i].list(j));
          take_counter++;
          if (take_counter == comm->take_arg())
          {
            break_flag = 1;
            break;
          }
        }
      }
    }
    else if ((comm->action_type() == "COUNT" && comm->isfile() == false))
    {
      long sum = 0;

      for (int i = 0; i < no_of_partitions; i++)
      {
        sum = sum + strtoll(reply[i].list(1).c_str(), NULL, 10);
      }
      fs->add_list(std::to_string(sum));
    }
    else if ((comm->action_type() == "REDUCE" && comm->isfile() == true))
    {
      std::vector<std::string> temp_vec;

      for (int i = 0; i < no_of_partitions; i++)
      {
        std::ifstream myfile;
        myfile.open("test_results" + std::to_string(i) + ".dat");
        if (myfile.is_open())
        {
          std::string tp;
          int i = 0;
          while (getline(myfile, tp))
          { //read data from file object and put it into string.
            temp_vec.push_back(tp);
          }
          myfile.close(); //close the file object.
        }
        //sum = sum + strtoll(reply[i].list(0).c_str(), NULL, 10);
      }
      RDD<std::string> mRdd = RDDParallelize::parallelize<std::string>(temp_vec);
      mRdd.Type = "STRING";
      FunctionFactory::createFunctions({"x"}, {"TOKEN"}, reply[0].list(0));
      std::vector<std::string> codes;
      std::vector<std::string> types;

      for (int j = 0; j < comm->command_list(comm->command_list_size() - 1).code_list_size(); j++)
      {
        codes.push_back(comm->command_list(comm->command_list_size() - 1).code_list(j));
        types.push_back(comm->command_list(comm->command_list_size() - 1).type_list(j));
      }
      FunctionFactory::createFunctions(codes, types, comm->command_list(comm->command_list_size() - 1).resulttype());
      FunctionFactory::compileFunctions();

      std::queue<Maps> maps2;
      for (int i = 0; i < FunctionFactory::getSize(); i++)
      {
        auto far = FunctionFactory::getFunction();
        maps2.push({std::get<0>(far), std::get<1>(far)});
      }
      auto results = mRdd.parseMaps(maps2);
      for (auto &a : results)
      {
        fs->add_list(a);
      }
      FunctionFactory::closeLib();
    }
    else if ((comm->action_type() == "REDUCE" && comm->isfile() == false))
    {
      std::vector<std::string> temp_vec;

      for (int i = 0; i < no_of_partitions; i++)
      {

        temp_vec.push_back(reply[i].list(1));
      }
      RDD<std::string> mRdd = RDDParallelize::parallelize<std::string>(temp_vec);
      mRdd.Type = "STRING";
      FunctionFactory::createFunctions({"x"}, {"TOKEN"}, reply[0].list(0));
      std::vector<std::string> codes;
      std::vector<std::string> types;

      for (int j = 0; j < comm->command_list(comm->command_list_size() - 1).code_list_size(); j++)
      {
        codes.push_back(comm->command_list(comm->command_list_size() - 1).code_list(j));
        types.push_back(comm->command_list(comm->command_list_size() - 1).type_list(j));
      }
      FunctionFactory::createFunctions(codes, types, comm->command_list(comm->command_list_size() - 1).resulttype());
      FunctionFactory::compileFunctions();

      std::queue<Maps> maps2;
      for (int i = 0; i < FunctionFactory::getSize(); i++)
      {
        auto far = FunctionFactory::getFunction();
        maps2.push({std::get<0>(far), std::get<1>(far)});
      }
      auto results = mRdd.parseMaps(maps2);
      for (auto &a : results)
      {
        fs->add_list(a);
      }
      FunctionFactory::closeLib();
    }
    else if ((comm->action_type() == "REDUCE_BY_KEY" && comm->isfile() == true))
    {
      std::vector<std::string> temp_vec_key;
      std::vector<std::string> temp_vec_value;

      for (int i = 0; i < no_of_partitions; i++)
      {
        std::ifstream myfile;
        myfile.open("test_results" + std::to_string(i) + ".dat");
        if (myfile.is_open())
        {
          std::string tp;
          int i = 0;
          std::stringstream ss;
          while (getline(myfile, tp))
          { //read data from file object and put it into string.
            ss.str(tp);
            std::string key;
            std::string value;
            ss >> key;
            ss >> value;
            temp_vec_key.push_back(key);
            temp_vec_value.push_back(value);
            ss.clear();
          }
          myfile.close(); //close the file object.
        }
        //sum = sum + strtoll(reply[i].list(0).c_str(), NULL, 10);
      }
      RDD<std::pair<std::string, std::string>> mRdd = RDDParallelize::parallelizePair<std::string, std::string>(temp_vec_key, temp_vec_value);
      mRdd.Type = "TUPLE STRING STRING";
      FunctionFactory::createFunctions({"x", "._", "1"}, {"TOKEN", "TUPLE_ACCESS", "NUM"}, "TUPLE STRING STRING");
      FunctionFactory::createFunctions({"x", "._", "2"}, {"TOKEN", "TUPLE_ACCESS", "NUM"}, "TUPLE STRING STRING");
      FunctionFactory::createFunctions({"x", "._", "1"}, {"TOKEN", "TUPLE_ACCESS", "NUM"}, reply[0].list(0));
      FunctionFactory::createFunctions({"x", "._", "2"}, {"TOKEN", "TUPLE_ACCESS", "NUM"}, reply[0].list(0));
      std::vector<std::string> codes;
      std::vector<std::string> types;

      for (int j = 0; j < comm->command_list(comm->command_list_size() - 1).code_list_size(); j++)
      {
        codes.push_back(comm->command_list(comm->command_list_size() - 1).code_list(j));
        types.push_back(comm->command_list(comm->command_list_size() - 1).type_list(j));
      }
      FunctionFactory::createFunctions(codes, types, comm->command_list(comm->command_list_size() - 1).resulttype());
      FunctionFactory::compileFunctions();

      std::queue<Maps> maps2;
      for (int i = 0; i < FunctionFactory::getSize(); i++)
      {
        auto far = FunctionFactory::getFunction();
        maps2.push({std::get<0>(far), std::get<1>(far)});
      }
      auto results = mRdd.parseMaps(maps2);
      for (auto &a : results)
      {
        fs->add_list(a);
      }
      FunctionFactory::closeLib();
    }
    else if ((comm->action_type() == "REDUCE_BY_KEY" && comm->isfile() == false))
    {
      std::vector<std::string> temp_vec_key;
      std::vector<std::string> temp_vec_value;

      std::stringstream ss;
      for (int i = 0; i < no_of_partitions; i++)
      {
        for (int j = 1; j < reply[i].list_size(); j++)
        {
          ss.str((reply[i].list(j)));
          std::string key;
          std::string value;
          ss >> key;
          ss >> value;
          temp_vec_key.push_back(key);
          temp_vec_value.push_back(value);
          ss.clear();
        }
      }
      RDD<std::pair<std::string, std::string>> mRdd = RDDParallelize::parallelizePair<std::string, std::string>(temp_vec_key, temp_vec_value);
      mRdd.Type = "TUPLE STRING STRING";
      FunctionFactory::createFunctions({"x", "._", "1"}, {"TOKEN", "TUPLE_ACCESS", "NUM"}, "TUPLE STRING STRING");
      FunctionFactory::createFunctions({"x", "._", "2"}, {"TOKEN", "TUPLE_ACCESS", "NUM"}, "TUPLE STRING STRING");
      FunctionFactory::createFunctions({"x", "._", "1"}, {"TOKEN", "TUPLE_ACCESS", "NUM"}, reply[0].list(0));
      FunctionFactory::createFunctions({"x", "._", "2"}, {"TOKEN", "TUPLE_ACCESS", "NUM"}, reply[0].list(0));
      std::vector<std::string> codes;
      std::vector<std::string> types;

      for (int j = 0; j < comm->command_list(comm->command_list_size() - 1).code_list_size(); j++)
      {
        codes.push_back(comm->command_list(comm->command_list_size() - 1).code_list(j));
        types.push_back(comm->command_list(comm->command_list_size() - 1).type_list(j));
      }
      FunctionFactory::createFunctions(codes, types, comm->command_list(comm->command_list_size() - 1).resulttype());
      FunctionFactory::compileFunctions();

      std::queue<Maps> maps2;
      for (int i = 0; i < FunctionFactory::getSize(); i++)
      {
        auto far = FunctionFactory::getFunction();
        maps2.push({std::get<0>(far), std::get<1>(far)});
      }
      auto results = mRdd.parseMaps(maps2);
      for (auto &a : results)
      {
        fs->add_list(a);
      }
      FunctionFactory::closeLib();
    }
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
  //handshake();
  RunServer(argv[1]);

  return 0;
}
