#ifndef avl_tree
#define avl_tree

#include<string>
#include<iostream>
using namespace std;

struct Node {
    int key;
    string ip_plus_port;
    Node* left;
    Node* right; 
    Node():key(0),ip_plus_port(""),left(NULL),right(NULL){}
    Node(int key,string ip_plus_port,Node* left,Node* right)
    :key(key),ip_plus_port(ip_plus_port),left(left),right(right){}
}; 

Node *root=NULL;

class avltree {
public:
    // find min value of the tree
    Node* minValue(Node* node) { 
        Node* current = node; 
        while (current->left != NULL) { 
            current = current->left; 
        } 
        return current; 
    }    

    // find max value of the tree
    Node* maxValue(Node* node) { 
        Node* current = node; 
        while (current->right != NULL) { 
            current = current->right; 
        } 
        return current;
    } 

    // give succ and pre
    void Suc(Node* root1, Node*& pre, Node*& suc, int key) {        
        if (root1 == NULL)  
            return ; 
        if (root1->key == key) { //base case
            if (root1->left != NULL) {    
                Node* tmp = root1->left; 
                while (tmp->right) 
                    tmp = tmp->right; 
                pre = tmp ; 
            }  
            if (root1->right != NULL) { 
                Node* tmp = root1->right ; 
                while (tmp->left) 
                    tmp = tmp->left ; 
                suc = tmp ; 
            } 
            return  ; 
        } 

        if (root1->key > key) { 
            suc = root1 ; 
            Suc(root1->left, pre, suc, key) ; 
        } 
        else { 
            pre = root1 ; 
            Suc(root1->right, pre, suc, key) ;   
        } 
    } 

    Node *insert(Node *node, int key, string ip_plus_port) {
        if (node == NULL) {//base case
            node = new Node(key,ip_plus_port,NULL,NULL);
            return node; 
        }
        if (key < node->key) { 
            node->left  = insert(node->left, key,ip_plus_port); 
        }
        else {
            node->right = insert(node->right, key,ip_plus_port); 
        }
        return node; 
    } 

    Node *deleteNode(Node *root1, int key) {
        if (root1 == NULL)
            return root1;
        if (key < root1->key)
            root1->left = deleteNode(root1->left, key);
        else if (key > root1->key)
            root1->right = deleteNode(root1->right, key);
        else {
            if (root1->left == NULL) {
                Node *temp = root1->right;
                free(root1);
                return temp;
            }
            else if (root1->right == NULL) {
                Node *temp = root1->left;
                free(root1);
                return temp;
            }
            Node *temp = minValue(root1->right);
            root1->key = temp->key;
            root1->ip_plus_port=temp->ip_plus_port;
            root1->right = deleteNode(root1->right, temp->key);
        }
        return root1;
    }

    void  inorder(Node *root1) {
        if(root1==NULL) {
            return;
        }
        else {
            inorder(root1->left);
            cout << "hash code " <<root1->key << " " << root1->ip_plus_port << endl;
            inorder(root1->right);
        }
    }
};

#endif 