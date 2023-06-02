#ifndef COMMON_FUNCN
#define COMMON_FUNCN
#include<bits/stdc++.h>
#include<stdio.h>
#include<sys/socket.h>
#include<arpa/inet.h> 
#include<sys/types.h> 
#include<errno.h>
#include<unistd.h> 
#include<cstring>
#include<pthread.h>
using namespace std;

#define RING_SIZE 100
#define BUFF_SIZE 1024
#define UDP_PORT 3769

const uint32_t CRC32_POLY = 0xEDB88320;
int consistent_hash(string s)
{   
	uint32_t crc = 0xFFFFFFFF;
    for (char c : s) {
        crc ^= static_cast<uint32_t>(c);
        for (int i = 0; i < 8; ++i) {
            crc = (crc >> 1) ^ (CRC32_POLY & (-(crc & 1)));
        }
    }
    return (~crc)%RING_SIZE;
}

int initialize_socket(string ip,string port)
{
	int port1=stoi(port);
	const char *ip1=ip.c_str();

	//socket create
	int sock_fd=socket(AF_INET,SOCK_STREAM,0);
	if(sock_fd<0)
	{
		perror("ERROR IN SOCKET CREATION");
	}

	//setopt 
	int opt=3;
	int setopt=setsockopt(sock_fd,SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT ,&opt,sizeof(opt));
		if(setopt<0)
		{
			perror(" SOCKOPT FAILED");	
		}

	//create structure
	struct sockaddr_in socket_struct;
	socket_struct.sin_family=AF_INET;
	socket_struct.sin_addr.s_addr =inet_addr(ip1);
	socket_struct.sin_port=htons(port1);

	//bind
	int bd=bind(sock_fd,(struct sockaddr *)&socket_struct,sizeof(socket_struct));

	if(bd<0)
	{
		perror("BIND FAILED");
	}
	return sock_fd;
}

//create a new socket has local ip + random port
int initialize_socket_without_bind()
{
	int sock_fd=socket(AF_INET,SOCK_STREAM,0);
	if(sock_fd<0)
	{
		perror("ERROR IN SOCKET CREATION");
	}
	//setopt 
	int opt=3;
	int setopt=setsockopt(sock_fd,SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT ,&opt,sizeof(opt));
	if(setopt<0)
	{
		perror(" SOCKOPT FAILED");		
	}
	return sock_fd;
}

void connect_f(int& sock_fd, string ip,string port)
{
	int port1=stoi(port);
	const char *ip1=ip.c_str();

	cout<<"trying to connect "<<ip1<<" "<<port1<<endl;
	struct sockaddr_in ip_server;
	ip_server.sin_family=AF_INET;
	ip_server.sin_addr.s_addr =inet_addr(ip1);
	ip_server.sin_port=htons(port1);

	int con=connect(sock_fd,(struct sockaddr*)&ip_server,sizeof(ip_server));
	if(con<0)
	{
		sock_fd=-1;
		perror("CANNOT CONNECT");
	}
} 

void send_message(int sock_fd, string  msg)
{ 
  	char *char_pointer=(char*)msg.c_str();
  	char char_array[BUFF_SIZE];
  	strcpy(char_array,char_pointer);
  	send(sock_fd,char_pointer,sizeof(char_array),0);
}

string receive_message(int ser_fd)
{   
	char Received_msg[BUFF_SIZE]={0};
    recv(ser_fd,Received_msg,sizeof(Received_msg),0);
    return string(Received_msg);
}

#endif