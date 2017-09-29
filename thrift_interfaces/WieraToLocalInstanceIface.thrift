service WieraToLocalInstanceIface {
	string peersInfo(1:string strPolicy);
	string ping();
	string policyChange(1:string strPolicy);
	string dataPlacementChange(1:string strDataPlacement);
}
