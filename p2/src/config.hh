#ifndef CONFIG_HPP
#define CONFIG_HPP


using namespace std;

#define SERVER1 "0.0.0.0:50052"
#define SERVER2 "ms1220.utah.cloudlab.us:50052"
#define SERVER3 "0.0.0.0:50055"
#define SERVER4 "0.0.0.0:50056"
#define SERVER5 "0.0.0.0:50057"
#define SERVER_CNT 5

string serverIPs[SERVER_CNT] = {SERVER1, SERVER2, SERVER3, SERVER4, SERVER5};

#endif