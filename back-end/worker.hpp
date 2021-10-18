
class Worker {       
    int id;       
    std::string IP;  
    std::string port;

public:
    Worker(std::string IP, std::string port, int id) { // Constructor with parameters
      this->IP = IP;
      this->port = port;
      this->id = id;
    }

    int getid(){
      return id;
    }
    std::string getIP(){
      return IP;
    }
    std::string getport(){
      return port;
    }
};