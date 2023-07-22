#include <bits/stdc++.h>
#include "COMMON_FUNCN.h"
#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "jsonstring.h"
#include <semaphore.h>

using namespace rapidjson;
using namespace std;
#define FILENAME "cs_config.txt"

Document doc;
map<string, string> ownmap;
map<string, string> prevmap;

struct thread_data
{
	string ip;
	string port;
};

bool check_key_present(string table,string key)
{
	if(table=="own")
	{
		if(ownmap.find(key)!=ownmap.end())
			return true;
		else
			return false;
	}
	else if(table=="prev")
	{
		if(prevmap.find(key)!=prevmap.end())
			return true;
		else
			return false;
	}
	return false;
}

void print_map()
{
	cout<<"------------------Printing ownmap----------------------"<<endl;
	for(auto x: ownmap){
		cout<<x.first<<"	"<<x.second<<endl;
	}
	cout<<"------------------Printing prevmap---------------------"<<endl;
	for(auto x: prevmap){
		cout<<x.first<<"	"<<x.second<<endl;
	}
	cout<<"-------------------------------------------------------"<<endl;
}

string handle_get(string jsonFromCS)
{
	//parse for table and key
	assert(doc.HasMember("table"));
	assert(doc.HasMember("key"));
	assert(doc["table"].IsString());
	assert(doc["key"].IsString());
			
	string table=doc["table"].GetString();
	string key=doc["key"].GetString();
	string fail="key_error";

	if(table=="own")
	{
		if(check_key_present(table,key)==false)
		{
			return fail;
		}
		return ownmap[key];
	}
	else if(table=="prev")
	{
		if(check_key_present(table,key)==false)
		{
			return fail;
		}
		return prevmap[key];
	}
	return "";
}

string handle_put(string jsonFromCS)
{
	assert(doc.HasMember("table"));
	assert(doc.HasMember("key"));
	assert(doc.HasMember("value"));
	assert(doc["table"].IsString());
	assert(doc["key"].IsString());
	assert(doc["value"].IsString());

	string table=doc["table"].GetString();
	string key=doc["key"].GetString();
	string value=doc["value"].GetString();

	string success="put_success";
	string fail="key_error";

	if(table=="own")
	{
		ownmap[key]=value;
		return success;
	}
	else if(table=="prev")
	{
		prevmap[key]=value;
		return success;
	}
	return "";
}

string handle_update(string jsonFromCS)
{
	assert(doc.HasMember("table"));
	assert(doc.HasMember("key"));
	assert(doc.HasMember("value"));
	assert(doc["table"].IsString());
	assert(doc["key"].IsString());
	assert(doc["value"].IsString());

	string table=doc["table"].GetString();
	string key=doc["key"].GetString();
	string value=doc["value"].GetString();

	string success="update_success";
	string fail="key_error";

	if(table=="own")
	{
		if(check_key_present(table,key)==false)
		{
			return fail;
		}
		ownmap[key]=value;
		return success;
	}
	else if(table=="prev")
	{
		if(check_key_present(table,key)==false)
		{
			return fail;
		}
		prevmap[key]=value;
		return success;
	}
	return "";
}

string handle_delete(string jsonFromCS)
{
	assert(doc.HasMember("table"));
	assert(doc.HasMember("key"));
	assert(doc["table"].IsString());
	assert(doc["key"].IsString());

	string table=doc["table"].GetString();
	string key=doc["key"].GetString();

	string success="delete_success";
	string fail="key_error";

	if(table=="own")
	{
		if(check_key_present(table,key)==false)
		{
			return fail;
		}
		ownmap.erase(key);
		return success;
	}
	else if(table=="prev")
	{
		if(check_key_present(table,key)==false)
		{
			return fail;
		}
		prevmap.erase(key);
		return success;
	}
	return "";
}


//------------------handle_leader_functions--------------------------------------------------------------------------------------- 

void send_table(int fd, string table)
{
	string resp="";
	if(table=="own"){
		for(auto x:ownmap){
			resp+=x.first+":"+x.second+"|";
		}
		resp=resp.substr(0,resp.length()-1);		
	}
	else if(table=="prev"){
		for(auto x:prevmap){
			resp+=x.first+":"+x.second+"|";
		}
		resp=resp.substr(0,resp.length()-1);	
	}
	send_message(fd, resp);
}

void receive_table(int sockfd, string table)
{
	string resp_table=receive_message(sockfd);
	if(resp_table=="" || resp_table.empty()){
		cout<<"Recieved empty table"<<endl;
		return;
	}
	vector<string> kv;
	char* keyval = strtok((char*)resp_table.c_str(), "|"); 
	kv.push_back(keyval);
	while ((keyval = strtok(NULL, "|"))!=NULL) {
        kv.push_back(keyval);
    }

    if(table=="own"){
    	ownmap.clear();
    	for(int i=0;i<kv.size();i++){
			char* s=(char*)kv[i].c_str();
			string key=strtok(s,":");
			string val=strtok(NULL,":");
			ownmap[key]=val;
		}
    }
    if(table=="prev"){
    	prevmap.clear();
    	for(int i=0;i<kv.size();i++){
			char* s=(char*)kv[i].c_str();
			string key=strtok(s,":");
			string val=strtok(NULL,":");
			prevmap[key]=val;
		}
    }
}

void update_own()
{
	for(auto it: prevmap)
	{
		string p_key=it.first;
		string p_value=it.second;
		ownmap[p_key]=p_value;
	}
	prevmap.clear();
}

int connect_pre(string ss_ip,string ss_port,string pre_ip1,string pre_port1,string role,string table)
{
	int sock_fd=initialize_socket_without_bind();
	connect_f(sock_fd,pre_ip1,pre_port1);
	send_message(sock_fd,update_table_SS(role,table));
	return sock_fd;
}

void handle_leader(string ss_ip,string ss_port,int coord_fd)
{
	assert(doc.HasMember("pre_ip"));
	assert(doc.HasMember("succ_of_succ_ip"));

	assert(doc["pre_ip"].IsString());
	assert(doc["succ_of_succ_ip"].IsString());

	string pre_ipport=doc["pre_ip"].GetString();
	string succ_of_succ_ipport=doc["succ_of_succ_ip"].GetString();

	char* pre_ip1=strtok((char*)pre_ipport.c_str(),":");
	char* pre_port1=strtok(NULL,":");
	char* succ_of_succ_ip1=strtok((char*)succ_of_succ_ipport.c_str(),":");
	char* succ_of_succ_port1=strtok(NULL,":");

	//move prev table of leader to own table of leader 
	update_own();

	//if predecessor equals to leader that means only one slave server is up
	if(ss_ip==pre_ip1&&ss_port==pre_port1)
	{
		send_message(coord_fd,send_message_ready("migration_completed"));
		return ;
	}
	
	//updating prev table of leader using own table of predecessor
	int sock_fd;
	sock_fd=connect_pre(ss_ip,ss_port,pre_ip1,pre_port1,"pre","own");
	receive_table(sock_fd,"prev");
	close(sock_fd); 

	//sending own table of leader to update prev table of succ_of_succ 
	sock_fd=connect_pre(ss_ip,ss_port,succ_of_succ_ip1,succ_of_succ_port1,"succ_of_succ","prev");
	receive_message(sock_fd);//msg recvd: ready_for_table
	send_table(sock_fd,"own");
	send_message(coord_fd,send_message_ready("migration_completed"));
	close(sock_fd);
}

//------------------------------------------------------------------------------------------------------------------

int new_ss_connect(string ss_ip,string ss_port,string succ_ip,string succ_port,string role,string pre_ip_to_send,string ss_ip_to_send,string succ_of_succ_ip_to_send)
{
	int sock_fd=initialize_socket_without_bind();
	connect_f(sock_fd,succ_ip,succ_port);
	send_message(sock_fd,inform_leader_migration(role,pre_ip_to_send,ss_ip_to_send,succ_of_succ_ip_to_send));
	return sock_fd;
}

void handle_new_ss_leader(string new_ss_ip,string new_ss_port,string mig_from_cs,Document& doc2)
{
	assert(doc2.HasMember("pre_ip"));
	assert(doc2.HasMember("succ_ip"));
	assert(doc2.HasMember("succ_of_succ_ip"));

	assert(doc2["pre_ip"].IsString());
	assert(doc2["succ_ip"].IsString());
	assert(doc2["succ_of_succ_ip"].IsString());

	string pre_ipport=doc2["pre_ip"].GetString();//has ip:port of predeceesor
	string succ_ipport=doc2["succ_ip"].GetString();
	string succ_of_succ_ipport=doc2["succ_of_succ_ip"].GetString();

	char* pre_ip1=strtok((char*)pre_ipport.c_str(),":");
	char* pre_port1=strtok(NULL,":");
	char* succ_ip1=strtok((char*)succ_ipport.c_str(),":");
	char* succ_port1=strtok(NULL,":");
	char* succ_of_succ_ip1=strtok((char*)succ_of_succ_ipport.c_str(),":");
	char* succ_of_succ_port1=strtok(NULL,":");
	
	//new slave server connects to its succ to get succ's prev into its own.
	int sockfd;
	string pre_ip_to_send=string(pre_ip1)+":"+string(pre_port1);
	string ss_ip_to_send=new_ss_ip+":"+new_ss_port;
	string succ_of_succ_ip_to_send=string(succ_of_succ_ip1)+":"+string(succ_of_succ_port1);
	sockfd=new_ss_connect(new_ss_ip,new_ss_port,succ_ip1,succ_port1,"new_ss_succ",pre_ip_to_send,ss_ip_to_send,succ_of_succ_ip_to_send);
	receive_table(sockfd,"own");

	//new slave server connects to its pre to get pre's own into its prev
	sockfd=new_ss_connect(new_ss_ip,new_ss_port,pre_ip1,pre_port1,"new_ss_pre",pre_ip_to_send,ss_ip_to_send,succ_of_succ_ip_to_send);
	receive_table(sockfd,"prev");
}

void update_new_ss_own(int llimit, int ulimit)
{
	prevmap.clear();
	vector<string> keys_to_delete;
	if(llimit<ulimit){
		for(auto x:ownmap){
			int hashed_key=consistent_hash(x.first);
			if(llimit<=hashed_key && hashed_key<ulimit)
			{
				prevmap.insert({x.first,x.second});
				keys_to_delete.push_back(x.first);
			}
		}
		for(int i=0;i<keys_to_delete.size();i++){
			ownmap.erase(keys_to_delete[i]);
		}
	}
	else if(llimit>ulimit){
		for(auto x:ownmap){
			int hashed_key=consistent_hash(x.first);
			if((llimit<=hashed_key && hashed_key<RING_SIZE) || (0<=hashed_key && hashed_key<ulimit))
			{
				prevmap.insert({x.first,x.second});
				keys_to_delete.push_back(x.first);
			}
		}
		for(int i=0;i<keys_to_delete.size();i++){
			ownmap.erase(keys_to_delete[i]);
		}
	}
	else{
		cout<<"Amazing! llimit==ulimit"<<endl;
	}
}

void handle_new_ss_succ(string jsonFromSS, string ip_address,string port_number)
{
	assert(doc.HasMember("pre_ip"));
	assert(doc.HasMember("succ_ip"));
	assert(doc.HasMember("succ_of_succ_ip"));

	assert(doc["pre_ip"].IsString());
	assert(doc["succ_ip"].IsString());
	assert(doc["succ_of_succ_ip"].IsString());

	string pre_ipport=doc["pre_ip"].GetString();
	string new_ss_ipport=doc["succ_ip"].GetString();
	string succ_of_succ_ipport=doc["succ_of_succ_ip"].GetString();

	string succ_of_succ_ipport1=succ_of_succ_ipport;
	char* succ_of_succ_ip1=strtok((char*)succ_of_succ_ipport1.c_str(),":");
	char* succ_of_succ_port1=strtok(NULL,":");
	
	int lower_limit=consistent_hash(pre_ipport);
	int upper_limit=consistent_hash(new_ss_ipport);
	update_new_ss_own(lower_limit, upper_limit);

	if(ip_address==succ_of_succ_ip1 && port_number==succ_of_succ_port1){
		return;
	}
	int fd=new_ss_connect(ip_address, port_number, succ_of_succ_ip1, succ_of_succ_port1, "new_ss_succ_of_succ", "","","");
	string rmsg=receive_message(fd);//recvd msg :  ack + ready_for_table

	send_table(fd, "own");
	string rmsg1=receive_message(fd); //recvd msg : ack + new_ss_succ_of_succ_done
	close(fd);
}

//-----------------------------------heartbeat thread for SS----------------------------------------------------------------------------

void *heartbeat_conn(void *ptr){
	string ack;
	thread_data *tdata=(thread_data*)ptr;
	string ss_ip=tdata->ip;
	string ss_port=tdata->port;
	int fd=initialize_socket(ss_ip, ss_port);

	//Reading ip and port of CS from file
	string cs_ip,cs_port;
	ifstream file(FILENAME);
	if (file.is_open()) {	    
	    getline(file, cs_ip);
	    getline(file, cs_port); 
	    file.close();
	}
	cout<<"cs ip: "<<cs_ip<<" port "<<cs_port<<endl;

	//Connecting to CS
	connect_f(fd, cs_ip, cs_port);
	string x=receive_message(fd);//msg received: ack + connected
	send_message(fd, identity_string("slave_server"));
	string recv_msg=receive_message(fd);//1. msg received: ack + registration_successful (case for 1st server)
										//2. msg received: ack + migration_new_server
	Document doc1;
	//parse receive msg
	if(doc1.Parse(recv_msg.c_str()).HasParseError()){
		cout << "Error while parsing the json string recvd from CS (for finding message). Try again. json string: " << recv_msg<<endl;
		send_message(fd, ack_data_string("ack", "parse_error"));
		pthread_exit(NULL);
	}

	assert(doc1.IsObject());
	assert(doc1.HasMember("message")); //checks if doc1 has member named "role"
	assert(doc1["message"].IsString());
	string message=doc1["message"].GetString();

	if(message=="migration_new_server")
	{
		send_message(fd, ack_data_string("ack","ready_for_migration"));//sending ack + ready_for_migration to CS
		string mig_from_cs=receive_message(fd);//msg recvd: new_SS_leader + pre + succ + succ_of_succ

		Document doc2;
		//error
		if(doc2.Parse(mig_from_cs.c_str()).HasParseError()){
			cout << "Error while parsing the json string recvd from CS (for finding role). Try again. json string: " << mig_from_cs << endl;
			send_message(fd, ack_data_string("ack", "parse_error"));
			pthread_exit(NULL);
		}

		assert(doc2.IsObject());
		assert(doc2.HasMember("role")); 
		assert(doc2["role"].IsString());
		string role=doc2["role"].GetString();

		if(role=="new_ss_leader")
		{
			handle_new_ss_leader(ss_ip,ss_port,mig_from_cs,doc2);
			send_message(fd,ack_data_string("ack","migration_ss_done"));
		}
	}
	close(fd);
	print_map();

	//make udp connection to cs for heartbeat
	while(1)
	{
		//make structure
		struct sockaddr_in servaddr;
		memset(&servaddr, '\0', sizeof(servaddr));
		servaddr.sin_addr.s_addr = inet_addr(cs_ip.c_str()); 
	    servaddr.sin_port = htons(UDP_PORT); 
	    servaddr.sin_family = AF_INET;  
	    // create datagram socket 
	    int udp_fd = socket(AF_INET, SOCK_DGRAM, 0); 
	    // connect to server 
	    if(connect(udp_fd, (struct sockaddr *)&servaddr, sizeof(servaddr)) < 0) 
	    { 
	        printf("\n Error : Connect Failed \n"); 
	        exit(0); 
	    } 
	    //send udp message
	    string to_send=ss_ip+":"+ss_port;
	    send_message(udp_fd,ack_data_string("heartbeat",to_send));
	    sleep(4);
	}
}

//------------------thread that serves requests that it'll get from CS-------------------------------------------------------
void *serve_request(void *ptr)
{
	thread_data *tdata=(thread_data *)ptr;
	string ip_address=tdata->ip;
	string port_number=tdata->port;

	int sock_fd=initialize_socket(ip_address,port_number);
	//listen
	if(listen(sock_fd,10)<0)
	{
		perror("LISTEN FAILED");
	}

	//accept 
	int ip_client_length,client_fd;
	struct sockaddr_in ip_client;
	while((client_fd=accept(sock_fd,(struct sockaddr *)&ip_client,(socklen_t*)&ip_client_length))>0)
	{
		string jsonFromCS=receive_message(client_fd);
		if(doc.Parse(jsonFromCS.c_str()).HasParseError()){
			cout << "Error while parsing the json string recvd from CS (for finding role). Try again. json string: " << jsonFromCS << endl;
			send_message(client_fd, ack_data_string("ack", "parse_error"));
			continue;
		}

		assert(doc.IsObject());
		assert(doc.HasMember("role")); 
		assert(doc["role"].IsString());
		string role=doc["role"].GetString();

		if(role=="get"){
			string val=handle_get(jsonFromCS);
			if(val=="key_error") 
				send_message(client_fd, ack_data_string("ack", val));
			else 
				send_message(client_fd, ack_data_string("data", val));
		}
		else if(role=="put"){
			string val=handle_put(jsonFromCS);
			send_message(client_fd, ack_data_string("ack", val));
		}
		else if(role=="update"){
			string val=handle_update(jsonFromCS);
			send_message(client_fd, ack_data_string("ack", val));//ack + update_success or key_error
		}
		else if(role=="delete"){
			string val=handle_delete(jsonFromCS);
			send_message(client_fd, ack_data_string("ack", val));//ack + delete_success or key_error
		}	
//-----------------------------------------------------------------------------------------------------------------------
		else if(role=="leader"){
			handle_leader(ip_address,port_number,client_fd);
		}
		else if(role=="pre"){
			assert(doc.HasMember("table")); 
			assert(doc["table"].IsString());
			string table=doc["table"].GetString();
			send_table(client_fd,table);
		}
		else if(role=="succ_of_succ"){
			send_message(client_fd,send_message_ready("ready_for_table"));
			assert(doc.HasMember("table")); 
			assert(doc["table"].IsString());
			string table=doc["table"].GetString();  
			receive_table(client_fd,table);
		}
//------------------------------------------------------------------------------------------------------------------------
		else if(role=="new_ss_pre"){
			send_table(client_fd,"own");
		}
		else if(role=="new_ss_succ"){
			handle_new_ss_succ(jsonFromCS,ip_address,port_number);
			send_table(client_fd, "prev");
		}
		else if(role=="new_ss_succ_of_succ"){
			send_message(client_fd, send_message_ready("ready_for_table"));
			receive_table(client_fd, "prev");
			send_message(client_fd, ack_data_string("ack", "new_ss_succ_of_succ_done"));
		}
		else{
			cout<<"Wrong input sent to SS by CS. Try again!"<<endl;
		}
		print_map();
	}
}

//------------------------------------------main-----------------------------------------------------------------------------------

int main(int argc,char **argv)
{
	if(argc<=2)
	{
		cout<<"enter ip port "<<endl;
		return 0;
	}

	//adding values to thred-data
	thread_data *ss_struct=new thread_data;
	ss_struct->ip=argv[1];
	ss_struct->port=argv[2];

	pthread_t heartbeat,serve_req;
	pthread_create(&heartbeat,NULL,heartbeat_conn,(void *)ss_struct);
	pthread_create(&serve_req,NULL,serve_request,(void *)ss_struct);

	pthread_join(heartbeat,NULL);
	pthread_join(serve_req,NULL);
}
