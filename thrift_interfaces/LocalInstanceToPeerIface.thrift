service LocalInstanceToPeerIface {
	string ping();
	string forwardPutRequest(1:string strPutReq);
	string put(1:string strReq);
	string get(1:string strReq);
	string getLatestVersion(1:string strKey);

	//for cluster mode
	string getClusterLock(1:string strLockReq);
	string releaseClusterLock(1:string strLockReq);
	string setLeader(1:string strLeaderHostnameReq);
}
