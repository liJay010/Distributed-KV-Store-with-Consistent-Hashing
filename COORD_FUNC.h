#ifndef COORD_FUNC
#define COORD_FUNC
#include <bits/stdc++.h>
#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "jsonstring.h"
#include "COMMON_FUNCN.h"
#include "GLOBAL_CS.h"
#include "avl.hpp"
#include "lru_cache.hpp"

using namespace std;
using namespace rapidjson;

void new_reg_migration(int sock_fd,string ip_port)
{
    int hash_value=consistent_hash(ip_port);
    avltree av;
    Node *pre=NULL,*succ=NULL,*pre1=NULL,*succ_of_succ=NULL; 

    // fetch predecessor and successor of new reg server
    av.Suc(root,pre,succ,hash_value);
    if(succ==NULL)
    {
        succ=av.minValue(root);
    }
    if(pre==NULL)
    {
        pre=av.maxValue(root);
    }

    // fetch pre1 and succ_of_succ of new reg succ
    av.Suc(root,pre1,succ_of_succ,succ->key);
    if(succ_of_succ==NULL)
    {
        succ_of_succ=av.minValue(root);
    } 
    if(pre1==NULL)
    {
        pre1=av.maxValue(root);
    }

    // send to new SS to proceed migration
    send_message(sock_fd,ack_data_string("ack","migration_new_server"));
    receive_message(sock_fd);   // receive msg: ack + "ready_for_migration"
    root=av.insert(root,hash_value,ip_port);
    send_message(sock_fd,inform_leader_migration("new_ss_leader",pre->ip_plus_port,succ->ip_plus_port, succ_of_succ->ip_plus_port));
    receive_message(sock_fd);   // receive message: "migration_ss_done" | "parse_error" 
}

//---------------------------------------------------------------------------------------------------------------

void* timer(void* threadargs)
{  
    while(1)
    {	 
        sleep(30);
        cout<<"-----------------------------------heartbeat_count----------------------------------------"<<endl;
        for(auto x:heartbeat_count)    
        {
            cout<<x.first<<" "<<x.second<<endl;
        }
        cout<<"--------------------------------------------------------------------------------------------"<<endl;
        vector<string> values_to_delete; 
        for(auto it=heartbeat_count.begin();it!=heartbeat_count.end();it++)
        {
   	        if(it->second==0)
   	        {
                data_migration=true;                
                cout<<"slave server "<<it->first<<" is migrating "<<endl;   
   	  	        int hash_key;
   	  	        hash_key=consistent_hash(it->first);
                avltree av;
                Node *pre=NULL,*succ=NULL,*succ_of_succ=NULL,*pre1=NULL;
                av.Suc(root,pre,succ,hash_key); 
                if(succ==NULL)
                {
                    succ=av.minValue(root);
                }
                if(pre==NULL)
                {
                    pre=av.maxValue(root);
                }
                av.Suc(root,pre1,succ_of_succ,succ->key);
                if(succ_of_succ==NULL)
                {
                    succ_of_succ=av.minValue(root);
                } 
                if(pre1==NULL)
                {
                    pre1=av.maxValue(root);
                }   
                
                char ar1[1024];
                char ar[1024];
                char ar2[1024];
                strcpy(ar,(char*)(succ->ip_plus_port).c_str());
                strcpy(ar1,(char*)(succ->ip_plus_port).c_str());
                strcpy(ar2,(char*)(succ_of_succ->ip_plus_port).c_str());
                root=av.deleteNode(root,hash_key);
                values_to_delete.push_back(it->first);
       
                char *ip_address=strtok(ar,":");
                char *port_number=strtok(NULL,":");       
                int sock_fd=initialize_socket_without_bind();      //socket fd created with succ SS as leader to proceed with migration
                connect_f(sock_fd,ip_address,port_number);

                send_message(sock_fd,inform_leader_migration("leader",pre->ip_plus_port,ar1,ar2));
                cout<<receive_message(sock_fd)<<endl; //msg recvd from leader(succ) SS: "migration_completed"  
       	    }
        }

        for(auto it=values_to_delete.begin(); it!=values_to_delete.end(); it++)
            heartbeat_count.erase(*it);

        for(auto x:heartbeat_count)
        {
            heartbeat_count[x.first]=0;
        }
        data_migration=false;
    }  
}

//------------------------------------------------------------------------------------------------------------------

void* heartbeat_func(void* threadargs)
{   
    int port_num=UDP_PORT;
    const char* ip_num=(((struct heartbeat_struct*)threadargs)->ip_cs).c_str();

    int connectfd;
    struct sockaddr_in sockaddr_struct;
    int len = sizeof(sockaddr_struct);

    if((connectfd = socket(AF_INET, SOCK_DGRAM, 0)) == 0)
    {
        perror("socket failed");
        pthread_exit(NULL);
    }
    
    // struct sockaddr_in ip_server;
    sockaddr_struct.sin_family = AF_INET;
    sockaddr_struct.sin_addr.s_addr =htonl(INADDR_ANY);
    sockaddr_struct.sin_port=htons(port_num);

    if(bind(connectfd, (struct sockaddr*)&sockaddr_struct, sizeof(sockaddr_struct)) < 0)
    {
        perror("Bind failed in UDP");
        pthread_exit(NULL);
    }

    while(1)
    {
        Document document1;
        string receive_val=receive_message(connectfd);     // received msg from SS: heartbeat + ip:port
        if(document1.Parse(receive_val.c_str()).HasParseError())
        {
            cout<<"error in request for client parsing string."<<endl;
        }
        else if(strcmp(document1["req_type"].GetString(),"heartbeat")==0)
        { 
            assert(document1.IsObject());
            assert(document1.HasMember("req_type"));
            assert(document1.HasMember("message"));

            string ip_port=document1["message"].GetString();
            heartbeat_count[ip_port]++;
        }
    }
}

//------------------------------------------------------------------------------------------------------------------

// write ip port to file for slave servers and clients to access
void write_to_file(string ip, string port)
{
	ofstream fo;
	fo.open("cs_config.txt");
	fo << ip +"\n" + port;
	fo.close();
}

//------------------------------------------------------------------------------------------------------------------

void register_slave_server(string ip_port)
{
	int hashed_ip=consistent_hash(ip_port);
	avltree av;
	root=av.insert(root,hashed_ip,ip_port);
	av.inorder(root);
}

//------------------------------------------------------------------------------------------------------------//

string get_from_slave(int sock_fd,string key,string table)
{   
    Document document1;
    // send key to get value from slave server
    send_message(sock_fd,get_delete_SS("get",key,table));
    string value=receive_message(sock_fd);
    if(!(document1.Parse(value.c_str()).HasParseError()))
    {
        if(strcmp(document1["req_type"].GetString(),"data")==0)
        {
            assert(document1.IsObject());
            assert(document1.HasMember("message"));
            string value_vv=document1["message"].GetString();
            cache.put_in_cache(key,value_vv);
        } 
    }
    return value;
}

//-----------------------------------------------------------------------------------------------------------------//

string put_on_slave(int sock_fd,string key,string value,string table)
{   
    // send key,value to get ack from slave server
    send_message(sock_fd,put_update_SS("put",key,value,table));
    string msg_from_slave=receive_message(sock_fd);     // received: ack+put_success
    return msg_from_slave;
}

bool put_is_success(string msg_from_slave)
{
    Document document1;
    if(!(document1.Parse(msg_from_slave.c_str()).HasParseError())) 
    {
        if(strcmp(document1["req_type"].GetString(),"ack")==0 && strcmp(document1["message"].GetString(),"put_success")==0)
        {
            return true;
        }
    }
    return false;
}

//-------------------------------------------------------------------------------------------------------------------------//

string delete_from_slave(int sock_fd,string key,string table)
{
    // send key to get value from SS
    send_message(sock_fd,get_delete_SS("delete",key,table));
    string value=receive_message(sock_fd);
    return value;
}

bool delete_is_success(string msg_from_slave)
{
    Document document1;
    if(!(document1.Parse(msg_from_slave.c_str()).HasParseError())) 
    {
        if(strcmp(document1["req_type"].GetString(),"ack")==0 && strcmp(document1["message"].GetString(),"delete_success")==0)
        {
            return true;
        }
    }
    return false;
}

//-------------------------------------------------------------------------------------------------------------------//

string update_on_slave(int sock_fd,string key,string value,string table)
{   
    // send key to get value from SS
    send_message(sock_fd,put_update_SS("update",key,value,table));
    string value1=receive_message(sock_fd);
    return value1;
}

bool update_is_success(string& msg_from_slave)
{
    Document document1;
    if(!(document1.Parse(msg_from_slave.c_str()).HasParseError())) 
    {
        if(strcmp(document1["req_type"].GetString(),"ack")==0 && strcmp(document1["message"].GetString(),"update_success")==0)
        {
            return true;
        }
    }
    return false;
}

//----------------------------------------------------------------------------------------------------------------------------//

void serve_get_request(int connectfd,string key,string ip_port_cs)
{   
    int hash_value=consistent_hash(key);
    cout<<"hashed code: "<<hash_value<<endl;

    if(root->left==NULL && root->right==NULL)
    {    
        char am[1024];
        strcpy(am,(char*)root->ip_plus_port.c_str());
        char* ip_root_char=strtok(am,":");
        char* port_root_char=strtok(NULL,":");   
        string root_ip=ip_root_char;
        string root_port=port_root_char;
        cout<<"fetched form AVL "<<root->key<<" "<<root->ip_plus_port<<endl;
        int sock_fd1=initialize_socket_without_bind();
        connect_f(sock_fd1,root_ip,root_port);
        string value_from_slave=get_from_slave(sock_fd1,key,"own");
        send_message(connectfd,value_from_slave);
        return;
    }

    // get IP and port of successor from AVL
    avltree av;
    Node *pre=NULL,*succ=NULL; 
    av.Suc(root,pre,succ,hash_value);
    if(succ==NULL)
    {
        succ=av.minValue(root);
    }
    if(pre==NULL)
    {
        pre=av.maxValue(root);
    }
    cout<<"fetched form AVL "<<succ->key<<" "<<succ->ip_plus_port<<endl;

    //extract ip and port of succ slave server
    char* ip_port_avl=(char*)(succ->ip_plus_port).c_str();
    char ar[1024];
    strcpy(ar,ip_port_avl);
    char* ip_address=strtok(ar,":");
    char* port_number=strtok(NULL,":");
    int sock_fd2=initialize_socket_without_bind();
    connect_f(sock_fd2,ip_address,port_number);
 
    // if succ slave server is not up:
    if(sock_fd2 < 0)
    {
        avltree av_sec;
        Node *pree=NULL,*succ_of_succ=NULL;
        string ipport_new_id=ip_port_avl;
        //hash succ that we've retrieved to find succ_of_succ
        int hash_value_succ_slave=consistent_hash(ipport_new_id);
        av_sec.Suc(root,pree,succ_of_succ,hash_value_succ_slave);
        if(succ_of_succ==NULL)
        {
            succ_of_succ=av_sec.minValue(root);
        } 
        if(pree==NULL)
        {
            pree=av_sec.maxValue(root);
        }   

        char arr[1024];
        char* ip_port_avl1=(char*)(succ_of_succ->ip_plus_port).c_str();
        strcpy(arr,ip_port_avl1);
        char* ip_address1=strtok(arr,":");
        char* port_number1=strtok(NULL,":");
        int sock_fd3=initialize_socket_without_bind();
        connect_f(sock_fd3,ip_address1,port_number1);

        string value=get_from_slave(sock_fd3,key,"prev");
        send_message(connectfd,value);
    } 
    // if succ slave server is up:
    else
    {
        string value=get_from_slave(sock_fd2,key,"own");
        send_message(connectfd,value);
    } 
}

//-------------------------------------------------------------------------------------------------------------------------------------//

void serve_put_request(int connectfd,string key,string value,string ip_port_cs)
{
    int hash_value=consistent_hash(key);
    cout<<"hashed code: "<<hash_value<<endl;

    if(root->left==NULL && root->right==NULL)
    {    
        char am[1024];
        strcpy(am,(char*)root->ip_plus_port.c_str());
        char* ip_root_char=strtok(am,":");
        char* port_root_char=strtok(NULL,":");   
        string root_ip=ip_root_char;
        string root_port=port_root_char;
        cout<<"fetched form AVL "<<root->key<<" "<<root->ip_plus_port<<endl;
        int sock_fd1=initialize_socket_without_bind();
        connect_f(sock_fd1,root_ip,root_port);
        string msg_from_slave=put_on_slave(sock_fd1,key,value,"own");
        if(put_is_success(msg_from_slave))
        {
            cache.put_in_cache(key,value);
        }
        send_message(connectfd,msg_from_slave);
        return;
    }
    
    // get IP and port of successor from AVL
    avltree av;
    Node *pre=NULL,*succ=NULL; 
    av.Suc(root,pre,succ,hash_value);
    if(succ==NULL)
    {
        succ=av.minValue(root);
    }
    if(pre==NULL)
    {
        pre=av.maxValue(root);
    }
    cout<<"fetched form AVL "<<succ->key<<" "<<succ->ip_plus_port<<endl;

    //extract ip and port of succ slave server
    char* ip_port_avl=(char*)(succ->ip_plus_port).c_str();
    char ar[1024];
    strcpy(ar,ip_port_avl); 
    char* ip_address=strtok(ar,":");
    char* port_number=strtok(NULL,":");
    int hash_value1=consistent_hash(succ->ip_plus_port);

    // get IP and port of succ_of_succ from AVL
    avltree av1;
    Node *pre1=NULL,*succ1=NULL; 
    av.Suc(root,pre1,succ1,hash_value1);
    if(succ1==NULL)
    {
        succ1=av1.minValue(root);
    } 
    if(pre1==NULL)
    {
        pre1=av1.maxValue(root);
    }   

    char arr[1024];
    char* ip_port_avl1=(char*)(succ1->ip_plus_port).c_str();
    strcpy(arr,ip_port_avl1);
    char* ip_address1=strtok(arr,":");
    char* port_number1=strtok(NULL,":");

    // try to connect succ slave server
    int sock_fd=initialize_socket_without_bind();
    connect_f(sock_fd,ip_address,port_number);
    
    // try to connect succ_of_succ slave server
    int sock_fd1=initialize_socket_without_bind();
    connect_f(sock_fd1,ip_address1,port_number1);
    
    //Ensuring 2 phase commit -- if it is able to connect to both succ and succ_of_succ then only put success
    if(sock_fd > 0 && sock_fd1 > 0)
    {
        // put on succ
        string st1=put_on_slave(sock_fd,key,value,"own");
        //put on succ_of_succ
        string st2=put_on_slave(sock_fd1,key,value,"prev");
        if(put_is_success(st1)&&put_is_success(st2))
        {
            cache.put_in_cache(key,value);
            send_message(connectfd,ack_data_string("ack","put_success"));
        }
        else
            send_message(connectfd,ack_data_string("ack","commit_failed"));
    }
    else
        send_message(connectfd,ack_data_string("ack","commit_failed"));
}

//----------------------------------------------------------------------------------------------------------------------------------//

void serve_delete_request(int connectfd,string key,string ip_port_cs)
{
    int hash_value=consistent_hash(key);
    cout<<"hashed code: "<<hash_value<<endl;

    // if only one slave server is present
    if(root->left==NULL && root->right==NULL)
    {    
        char am[1024];
        strcpy(am,(char*)root->ip_plus_port.c_str());
        char* ip_root_char=strtok(am,":");
        char* port_root_char=strtok(NULL,":");   
        string root_ip=ip_root_char;
        string root_port=port_root_char;
        cout<<"fetched form AVL "<<root->key<<" "<<root->ip_plus_port<<endl;
        int sock_fd1=initialize_socket_without_bind();
        connect_f(sock_fd1,root_ip,root_port);
        string msg_from_slave=delete_from_slave(sock_fd1,key,"own");
        if(delete_is_success(msg_from_slave))
        {
            cache.delete_from_cache(key);
        }
        send_message(connectfd,msg_from_slave);
        return;
    }

    // get IP and port of succ from AVL
    avltree av;
    Node *pre=NULL,*succ=NULL; 
    av.Suc(root,pre,succ,hash_value);
    if(succ==NULL)
    {
        succ=av.minValue(root);
    }
    if(pre==NULL)
    {
        pre=av.maxValue(root);
    }
    cout<<"fetched form AVL "<<succ->key<<" "<<succ->ip_plus_port<<endl;

    //extract ip and port of succ slave server
    char* ip_port_avl=(char*)(succ->ip_plus_port).c_str();
    char ar[1024];
    strcpy(ar,ip_port_avl);
    char* ip_address=strtok(ar,":");
    char* port_number=strtok(NULL,":");
    int sock_fd1=initialize_socket_without_bind();
    connect_f(sock_fd1,ip_address,port_number);
    
    //get IP and port of succ_of_succ from AVL
    int hash_value1=consistent_hash(succ->ip_plus_port);
    cout<<"hashed code: "<<hash_value1<<endl;
    avltree av1;
    Node *pre1=NULL,*succ1=NULL; 
    av.Suc(root,pre1,succ1,hash_value1);
    if(succ1==NULL)
    {
        succ1=av1.minValue(root);
    } 
    if(pre1==NULL)
    {
        pre1=av1.maxValue(root);
    } 

    //extract ip and port of succ slave server
    char* ip_port_avl1=(char*)(succ1->ip_plus_port).c_str();
    char arr[1024];
    strcpy(arr,ip_port_avl1);
    char* ip_address1=strtok(arr,":");
    char* port_number1=strtok(NULL,":");
    int sock_fd2=initialize_socket_without_bind();
    connect_f(sock_fd2,ip_address1,port_number1);

    if(sock_fd1 > 0 && sock_fd2 > 0)
    {   
        //delete from own table of succ 
        string st1=delete_from_slave(sock_fd1,key,"own");
        //delete from prev table of succ_of_succ
        string st2=delete_from_slave(sock_fd2,key,"prev");
        if(delete_is_success(st1)&&delete_is_success(st2))
        {
            cache.delete_from_cache(key);
            send_message(connectfd,ack_data_string("ack","delete_success"));
        }
        else
            send_message(connectfd,ack_data_string("ack","commit_failed"));
    }
    else
        send_message(connectfd,ack_data_string("ack","commit_failed"));
}

//********************************************************************************************************************//

void serve_update_request(int connectfd,string key,string value,string ip_port_cs)
{  
    int hash_value=consistent_hash(key);
    cout<<"hashed code: "<<hash_value<<endl;

    if(root->left==NULL && root->right==NULL)
    {    
        char am[1024];
        strcpy(am,(char*)root->ip_plus_port.c_str());
        char* ip_root_char=strtok(am,":");
        char* port_root_char=strtok(NULL,":");   
        string root_ip=ip_root_char;
        string root_port=port_root_char;
        cout<<"fetched form AVL "<<root->key<<" "<<root->ip_plus_port<<endl;
        int sock_fd1=initialize_socket_without_bind();
        connect_f(sock_fd1,root_ip,root_port);

        string msg_from_slave=update_on_slave(sock_fd1,key,value,"own");
        if(update_is_success(msg_from_slave))
        {
            cache.put_in_cache(key,value);
        }
        send_message(connectfd,msg_from_slave);
        return;
    }

    // get IP and port of succ from AVL
    avltree av;
    Node *pre=NULL,*succ=NULL; 
    av.Suc(root,pre,succ,hash_value);
    if(succ==NULL)
    {
        succ=av.minValue(root);
    }
    if(pre==NULL)
    {
        pre=av.maxValue(root);
    }
    cout<<"fetched form AVL "<<succ->key<<" "<<succ->ip_plus_port<<endl;

    //extract ip and port of succ slave server
    char* ip_port_avl=(char*)(succ->ip_plus_port).c_str();
    char ar[1024];
    strcpy(ar,ip_port_avl); 
    char* ip_address=strtok(ar,":");
    char* port_number=strtok(NULL,":");
    int sock_fd1=initialize_socket_without_bind();
    connect_f(sock_fd1,ip_address,port_number);

    // get IP and port succ_of_succ  from AVL
    int hash_value1=consistent_hash(succ->ip_plus_port);
    avltree av1;
    Node *pre1=NULL,*succ1=NULL; 
    av.Suc(root,pre1,succ1,hash_value1);
    if(succ1==NULL)
    {
        succ1=av1.minValue(root);
    } 
    if(pre1==NULL)
    {
        pre1=av1.maxValue(root);
    }
    
    //extract ip and port of succ slave server
    char* ip_port_avl1=(char*)(succ1->ip_plus_port).c_str();
    char arr[1024];
    strcpy(arr,ip_port_avl1);
    char* ip_address1=strtok(arr,":");
    char* port_number1=strtok(NULL,":");
    int sock_fd2=initialize_socket_without_bind();
    connect_f(sock_fd2,ip_address1,port_number1);

    if(sock_fd1>0 && sock_fd2>0)
    {
        // update in own table of succ
        string st1=update_on_slave(sock_fd1,key,value,"own");
        // update in prev table of succ_of_succ
        string st2=update_on_slave(sock_fd2,key,value,"prev");
        if(update_is_success(st1)&&update_is_success(st2))
        {
            cache.put_in_cache(key,value);
            send_message(connectfd,ack_data_string("ack","update_success"));
        }
        else
            send_message(connectfd,ack_data_string("ack","commit_failed"));
    }
    else
        send_message(connectfd,ack_data_string("ack","commit_failed"));
}

//*****************************************************************************************************************************************//

void request_of_client(int connectfd,string ip_port_cs)
{   
    Document document1;
    string s;
    while(1)
    {
        s=receive_message(connectfd);        // received type: put,get,delete,update+key+value
        if(s.empty())
            return;
        cout<<"req received from client"<<s<<endl;
        
        // dont do anything if migration;
        while(data_migration==true);

        if(document1.Parse(s.c_str()).HasParseError())
        {
    	   cout<<"error in request for client parsing string"<<endl;
    	   send_message(connectfd,ack_data_string("ack","parse_error"));
        }
        else if(strcmp(document1["req_type"].GetString(),"get")==0)
        {   
            assert(document1.IsObject());
        	assert(document1.HasMember("key"));
            string key_v=document1["key"].GetString();
            string value1=cache.get_from_cache(key_v);
            if(!value1.empty())
            {
                cout<<"key "<<key_v<<" is present in cache"<<endl;
                send_message(connectfd,ack_data_string("data",value1)); 
            }
            else
            {
                cout<<"key "<<key_v<<" not present in cache. Fetching from SS"<<endl;                
                serve_get_request(connectfd,key_v,ip_port_cs);
            }  
        }
        else if(strcmp(document1["req_type"].GetString(),"put")==0)
        {  
            assert(document1.IsObject());
            assert(document1.HasMember("key"));
            assert(document1.HasMember("value"));
            string key_v=document1["key"].GetString();
            string value_v=document1["value"].GetString();
            serve_put_request(connectfd,key_v,value_v,ip_port_cs);
        } 
        else if(strcmp(document1["req_type"].GetString(),"delete")==0)
        {               
            assert(document1.IsObject());
            assert(document1.HasMember("key"));
            string key_v=document1["key"].GetString();
            serve_delete_request(connectfd,key_v,ip_port_cs);
        } 
        else if(strcmp(document1["req_type"].GetString(),"update")==0)
        {  
            assert(document1.IsObject());
            assert(document1.HasMember("key"));
            assert(document1.HasMember("value"));
            string key_v=document1["key"].GetString();
            string value_v=document1["value"].GetString();
            serve_update_request(connectfd,key_v,value_v,ip_port_cs);
        }  
        cout<<"-------------------------------------AVL--------------------------------------------------"<<endl; 
        avltree av;
        av.inorder(root); 
        cout<<"------------------------------------------------------------------------------------------"<<endl;  

        cout<<"------------------------------------Cache-------------------------------------------------"<<endl;  
        cache.display();
        cout<<"------------------------------------------------------------------------------------------"<<endl;
    }
}

#endif