service ApplicationToWieraIface {
    string startInstances(1:string policy);
    string stopInstances(1:string key);
    string getInstances(1:string key);
	string getLocalServerList();
}
