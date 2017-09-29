service WieraToLocalServerIface {
	string ping();
	string startInstance(1:string strPolicy);
	string stopInstance(1:string strPolicy);
}
