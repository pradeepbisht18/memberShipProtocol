/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
	if ( memberNode->bFailed ) {
		return false;
	}
	else {
		return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
	}
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
	Address joinaddr;
	joinaddr = getJoinAddress();

	// Self booting routines
	if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
		log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
		exit(1);
	}

	if( !introduceSelfToGroup(&joinaddr) ) {
		finishUpThisNode();
#ifdef DEBUGLOG
		log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
		exit(1);
	}

	return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
	// node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
	initMemberListTable(memberNode);

	return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {

	if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
		// I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
		log->LOG(&memberNode->addr, "Starting up group...");
#endif
		memberNode->inGroup = true;
	}
	else {
		int ret = sendMessageEmulnet(JOINREQ, joinaddr);
	}

	return 1;

}

int MP1Node::sendMessageEmulnet(MsgTypes type, Address *joinaddr){
	MessageHdr *msg;
#ifdef DEBUGLOG
	static char s[1024];
#endif
	size_t msgsize=0;
	if (type == HEARTBEAT){
		msgsize = sizeof(MessageHdr) + sizeof (joinaddr->addr)+ sizeof(long)+ 
						 sizeof (MemberListEntry) * memberNode->memberList.size() + 1;
		msg = (MessageHdr *) malloc(msgsize * sizeof(char));
		msg->msgType = type;
		memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
		int sz = memberNode->memberList.size();
		memcpy((char *)(msg+1) + sizeof(memberNode->addr.addr), &sz, sizeof(int));
		size_t offset = sizeof(memberNode->addr.addr) + sizeof(int);
		for (auto v: memberNode->memberList){
			memcpy((char *)(msg+1) + offset, &v, sizeof(v));
			offset += sizeof(v);
		}
	}
	else{
		msgsize = sizeof(MessageHdr) + sizeof (joinaddr->addr)+ sizeof(long) + 1;
		msg = (MessageHdr *) malloc(msgsize * sizeof(char));
		msg->msgType = type;
		memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
		memcpy((char *)(msg+1)+ sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));
	}
	emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);
	free (msg);
	return 1;
}
/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
	/*
	 * Your code goes here
	 */
	memberNode->memberList.clear();
	memberNode->inGroup = false;
	delete memberNode;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
	if (memberNode->bFailed) {
		return;
	}

	// Check my messages
	checkMessages();

	// Wait until you're in the group...
	if( !memberNode->inGroup ) {
		return;
	}

	// ...then jump in and share your responsibilites!
	nodeLoopOps();

	return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
	void *ptr;
	int size;

	// Pop waiting messages from memberNode's mp1q
	while ( !memberNode->mp1q.empty() ) {
		ptr = memberNode->mp1q.front().elt;
		size = memberNode->mp1q.front().size;
		memberNode->mp1q.pop();
		recvCallBack((void *)memberNode, (char *)ptr, size);
	}
	return;
}
Address MP1Node::getnewAddress(int id, short port) {
	Address joinaddr;
	memset(&joinaddr, 0, sizeof(Address));
	*(int *)(&joinaddr.addr) = id;
	*(short *)(&joinaddr.addr[4]) = port;

	return joinaddr;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
	/*Fetch the data from the data*/

	MessageHdr *msg = (MessageHdr *)data;
	int id;
	short port;
	long heartbeat;
	memcpy(&id, data + sizeof(MessageHdr), sizeof(int));
	memcpy(&port, data + sizeof(MessageHdr) + sizeof(int), sizeof(short));
	if (msg->msgType == JOINREQ) {
	memcpy(&heartbeat, data + sizeof(MessageHdr) + sizeof(int) + sizeof(short), sizeof(long));
		/*Insert the received process information into the member table*/
		long timestamp =  this->par->getcurrtime();
		MemberListEntry newNode(id, port, heartbeat, timestamp);

		if (!isPresent(newNode)){
			memberNode->memberList.push_back(newNode);
		}
		// Send JOINREP message
		Address newNodeAddress = getnewAddress(id, port);
#ifdef DEBUGLOG
		log->logNodeAdd(&memberNode->addr, &newNodeAddress);
#endif
		sendMessageEmulnet(JOINREP, &newNodeAddress);
	}
	if (msg->msgType == JOINREP) {
	memcpy(&heartbeat, data + sizeof(MessageHdr) + sizeof(int) + sizeof(short), sizeof(long));
		memberNode->inGroup = true;
		long timestamp =  this->par->getcurrtime();
		MemberListEntry newNode(id, port, heartbeat, timestamp);
		if (!isPresent(newNode)){
			memberNode->memberList.push_back(newNode);
		}
		Address newNodeAddress = getnewAddress(id, port);
#ifdef DEBUGLOG
		log->logNodeAdd(&memberNode->addr, &newNodeAddress);
#endif
	}
	if (msg->msgType == HEARTBEAT){
		int count =0;
		memcpy(&count, data + sizeof(MessageHdr) + sizeof(int) + sizeof(short), sizeof(int));
		updateSender(id, port);
		vector <MemberListEntry>recvList;
		size_t offset = sizeof(MessageHdr) + sizeof(int) + sizeof(short)+ sizeof(int);
		for(int i=0; i < count; i++){
			MemberListEntry tempNode;
			memcpy(&tempNode, data + offset, sizeof(MemberListEntry));
			recvList.push_back(tempNode);
			offset+= sizeof(MemberListEntry);
		}
		for(int i = 0; i < recvList.size(); ++i){
			MemberListEntry* node = getNode(recvList[i].id, recvList[i].port);  
			if(node != nullptr){
				if(recvList[i].heartbeat > node->heartbeat){
					node->heartbeat = recvList[i].heartbeat;
					node->timestamp = par->getcurrtime();
				}
			}else{
        		addMemberList(recvList[i].id, recvList[i].port);
			}
		}
	}
	return true;
}

void MP1Node::updateSender(int id, short port){
    
	MemberListEntry* src = getNode(id, port);
    if(src != nullptr){
        src->heartbeat ++;
        src->timestamp = par->getcurrtime();
    }else{
        addMemberList(id, port);
    }
}

void MP1Node::addMemberList(int id, short port) {
    long heartbeat = 1;
    long timestamp =  this->par->getcurrtime();
    MemberListEntry e(id, port, heartbeat, timestamp);
    memberNode->memberList.push_back(e);
    Address newNode = getnewAddress(id,port);
#ifdef DEBUGLOG
    log->logNodeAdd(&memberNode->addr ,&newNode);
#endif
}


MemberListEntry* MP1Node::getNode(int id, short port){
	for (int i = 0; i < memberNode->memberList.size(); i++){
        if(memberNode->memberList[i].id == id && memberNode->memberList[i].port == port)
            return &memberNode->memberList[i];
    } 
	return nullptr;
}
/**
 * FUNCTION NAME: isPresent
 */
bool MP1Node::isPresent(MemberListEntry node){
	for (auto val : memberNode->memberList){
		if (val.id == node.id && val.port == node.port)
			return true;
	}	
	return false;
}


/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

	/*
	 * Your code goes here
	 */
    //Increase the heartbeat
	memberNode->heartbeat++;
    for (int i= 0 ; i < memberNode->memberList.size(); ++i) {
        if(par->getcurrtime() - memberNode->memberList[i].timestamp >= TREMOVE) {
            Address removed_addr = getnewAddress(memberNode->memberList[i].id, memberNode->memberList[i].port);
            log->logNodeRemove(&memberNode->addr, &removed_addr);
            memberNode->memberList.erase(memberNode->memberList.begin()+i);
        }
    }
    // Send PING to the members of memberList
    for (int i = 0; i < memberNode->memberList.size(); i++) {
		int id = memberNode->memberList[i].id;
		short port = memberNode->memberList[i].port;
        Address newAddress = getnewAddress(memberNode->memberList[i].id, memberNode->memberList[i].port);
		sendMessageEmulnet(HEARTBEAT, &newAddress);
    }
	return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
	Address joinaddr;

	memset(&joinaddr, 0, sizeof(Address));
	*(int *)(&joinaddr.addr) = 1;
	*(short *)(&joinaddr.addr[4]) = 0;

	return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr){
	printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
			addr->addr[3], *(short*)&addr->addr[4]);
}
