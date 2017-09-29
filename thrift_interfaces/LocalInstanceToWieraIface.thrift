service LocalInstanceToWieraIface {
	string registerLocalInstance(1:string strInstanceInfo);
	string requestPolicyChange(1:string strPolicy);
	string updateMonitoringData(1:string strMonitoringData);
	string requestNewDataPlacementPolicy(1:string strRequest);
}
