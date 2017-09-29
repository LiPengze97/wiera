service ApplicationToLocalInstanceIface {
    bool put(1:string key, 2:binary value);
    bool update(1:string key, 2:i32 verison, 3:binary value);
    binary get(1:string key);
    binary getVersion(1:string key, 2:i32 version);
	binary getVersionList(1:string key);
    bool remove(1:string key);
    bool removeVersion(1:string key, 2:i32 version);
}
