service WieraThriftIface {
    string createInstances(1:string policy);
    string startInstances(1:string key);
    string stopInstances(1:string key);
    string getInstances(1:string key);
}
