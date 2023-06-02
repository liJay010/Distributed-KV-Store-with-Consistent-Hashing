#include<bits/stdc++.h>
#include "COORD_FUNC.h"
#include "GLOBAL_CS.h"
#include "COMMON_FUNCN.h"
#include "jsonstring.h"
#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
using namespace std;
using namespace rapidjson;

void* serve_requests(void* threadargs)
{
	pthread_detach(pthread_self());
	int connectfd=((struct thread_struct*)threadargs)->sockfd;
	string ip_port=((struct thread_struct*)threadargs)->ip_plus_port;
	string ip_port_cs=((struct thread_struct*)threadargs)->ip_port_CS;

	send_message(connectfd,ack_data_string("ack","connected"));
	string check_id=receive_message(connectfd); // message received identity + slave_server/client
	Document document;
	// If parsing gives error:
	if(document.Parse(check_id.c_str()).HasParseError())
	{
		cout<<"Error while parsing"<<endl;
		send_message(connectfd,ack_data_string("ack","parse_error"));
	}
    else if(strcmp(document["id"].GetString(),"client")==0)
    {	
    	cout<<"a client come"<<endl;
    	assert(document.IsObject());	
       	send_message(connectfd,ack_data_string("ack","ready_to_serve"));
       	request_of_client(connectfd,ip_port_cs);
    } 
    else if(strcmp(document["id"].GetString(),"slave_server")==0)
    {
       	cout<<"a slave server"<<endl;
       	assert(document.IsObject());
       	// when there is only one slave server
       	if(root == NULL)
       	{
       		register_slave_server(ip_port);
       		send_message(connectfd,ack_data_string("ack","registration_successful"));	// send message "registration_successful" for first slave server
       	} 
	   	else
       	{
       		// send message migration_new_server when more than one server in system
       		data_migration=true;
       		new_reg_migration(connectfd,ip_port);
       		data_migration=false;
       	}
	   	cout<<"-------------------------------------AVL--------------------------------------------------"<<endl; 
       	avltree av;
       	av.inorder(root); 
       	cout<<"------------------------------------------------------------------------------------------"<<endl;  
       	cout<<"registered"<<endl;
	}
}

int main(int argc,char **argv)
{
	string ip,port;
	if(argc<=2)
	{
		cout<<"enter ip port "<<endl;
		return 0;
	}
	ip=argv[1];
	port=argv[2];
	cout<<"cs ip "<<ip<<" port "<<port<<endl;
	write_to_file(ip,port);

    // pthread for heartbeat checking
	pthread_t heartbeat_thread;
	pthread_t time_thread;
    
	struct heartbeat_struct* hs=new heartbeat_struct();
	hs->ip_cs=ip;
	pthread_create(&heartbeat_thread,NULL,heartbeat_func,(void*)hs);

    // pthread for timer
	pthread_create(&time_thread,NULL,timer,NULL);

	int sock_fd=initialize_socket(ip,port);

	//listen
	if(listen(sock_fd,10)<0)
	{
		perror("LISTEN FAILED");
	}

	//create client server
	int connectfd;
	struct sockaddr_in ip_client;
	memset(&ip_client, '\0', sizeof(ip_client));
	int len;
	pthread_t tid;
	cout<<"ready to accept"<<endl;
	
	//one thread per customer
	while(1){
		connectfd=accept(sock_fd,(struct sockaddr *)&ip_client,(socklen_t*)&len);
		if(connectfd<0){
			perror("error in accept()");
			exit(1);
		}
		cout<<"accepted conn"<<endl;
		struct thread_struct* th=new thread_struct();
		th->sockfd=connectfd;
		char* ipclient=new char[INET_ADDRSTRLEN];
		inet_ntop(AF_INET,&(ip_client.sin_addr),ipclient,INET_ADDRSTRLEN);
		th->ip_plus_port=string(ipclient)+":"+to_string(ntohs(ip_client.sin_port));
		cout<<th->ip_plus_port<<endl;
		th->ip_port_CS=ip+":"+port;
		pthread_create(&tid,NULL,serve_requests,(void*)th);
	}
}