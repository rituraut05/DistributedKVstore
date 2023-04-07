#ifndef CONFIG_HPP
#define CONFIG_HPP


using namespace std;

#define SERVER1 "0.0.0.0:50052" // node1
#define SERVER2 "0.0.0.0:50053" // node2
#define SERVER3 "0.0.0.0:50054" // node3
#define SERVER4 "0.0.0.0:50055" // node4
#define SERVER5 "0.0.0.0:50056" // node5
// #define SERVER1 "ms1220.utah.cloudlab.us:50052" // node1
// #define SERVER2 "ms1116.utah.cloudlab.us:50052" // node2
// #define SERVER3 "ms1245.utah.cloudlab.us:50052" // node3
// #define SERVER4 "ms1242.utah.cloudlab.us:50052" // node4
// #define SERVER5 "ms1210.utah.cloudlab.us:50052" // node5
#define SERVER_CNT 5

string serverIPs[SERVER_CNT] = {SERVER1, SERVER2, SERVER3, SERVER4, SERVER5};

#endif