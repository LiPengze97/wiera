service wieraThrift {
    bool createInstances(1:string policy);
    bool startInstances(1:string key);
    bool stopInstances(1:string key);
    bool getInstances(1:string key);
}
