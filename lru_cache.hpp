#ifndef lru_cache
#define lru_cache

#include<iostream>
#include<string>
#include<unordered_map>
using namespace std;

class ListNode {
public:
	string key;
	string value;
	ListNode *prev;
	ListNode *next;
	ListNode():key(""),value(""),prev(NULL),next(NULL){}
	ListNode(string key, string value):key(key),value(value),prev(NULL),next(NULL) {}
};

class lrucache {
private:
    unordered_map<string,ListNode*> memo;
    ListNode* head=new ListNode;
    ListNode* tail=new ListNode;
    int capacity;

 	void pushToHead(ListNode* node)
	{
    	node->next=head->next;
		head->next->prev=node;
		node->prev=head;
		head->next=node;
 	}
 	void moveToHead(ListNode* node)
 	{
    	node->prev->next=node->next;
    	node->next->prev=node->prev;
    	pushToHead(node);
 	}
	void delTail(ListNode* node)
 	{
    	node->prev->next=tail;
    	tail->prev=node->prev;
 	}

public:
	lrucache(int capacity):capacity(capacity) {
        head->next=tail;
        tail->prev=head;
    }
	~lrucache() {
		for(auto& i:memo) {
			delete i.second;
		}
	}

	void display()
	{
		ListNode *cur=head->next;
		while(cur->next!=NULL)
		{
			cout<<"key: "<<cur->key<<" value: "<<cur->value<<endl;
			cur=cur->next;
		}
	}

 	void put_in_cache(string key,string value)
 	{
    	if(memo.count(key))
    	{
        	memo[key]->value=value;
        	moveToHead(memo[key]);
    	}
   	 	else
    	{
        	ListNode* node=new ListNode(key,value);
        	pushToHead(node);
        	memo[key]=node;
        	if(memo.size()>capacity)
        	{
            	ListNode* temp=tail->prev;
            	delTail(temp);
            	memo.erase(temp->key);
				delete temp;
        	}
    	}
 	} 

  	string get_from_cache(string key)
  	{
    	if(memo.count(key))
    	{
        	moveToHead(memo[key]);
        	return memo[key]->value;
    	}
    	else
    	{
        	return "";
    	}
  	}

	void delete_from_cache(string key)
	{	
  		if(memo.count(key))
  		{
	      	ListNode* del=memo[key];
	        del->prev->next=del->next;
			del->next->prev=del->prev;
			memo.erase(key);
			delete del;
      	} 
	}
};

lrucache cache(3);

#endif