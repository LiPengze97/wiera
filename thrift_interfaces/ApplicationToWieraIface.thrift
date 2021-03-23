service ApplicationToWieraIface {
    string startWieraInstance(1:string policy);
    string stopWieraInstance(1:string id);
    string getWieraInstance(1:string id);
	string getLocalServerList();
}
