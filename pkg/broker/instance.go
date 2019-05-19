package broker

import (
	"reflect"
)

type Stat struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type Instance struct {
	Id            string        `json:"id"`
	Name          string        `json:"name"`
	ProviderId    string        `json:"provider_id"`
	Plan          *ProviderPlan `json:"plan,omitempty"`
	Username      string        `json:"username"`
	Password      string        `json:"password"`
	Endpoint      string        `json:"endpoint"`
	Status        string        `json:"status"`
	Ready         bool          `json:"ready"`
	Engine        string        `json:"engine"`
	EngineVersion string        `json:"engine_version"`
	Scheme        string        `json:"scheme"`
}

type Entry struct {
	Id       string
	Name     string
	PlanId   string
	Claimed  bool
	Tasks	 int
	Status   string
	Username string
	Password string
	Endpoint string
}

func (i *Instance) Match(other *Instance) bool {
	return reflect.DeepEqual(i, other)
}

type ResourceUrlSpec struct {
	Username string
	Password string
	Endpoint string
	Plan     string
}

type ResourceSpec struct {
	Name string `json:"name"`
}

func IsAvailable(status string) bool {
	return status == "available" ||
			// gcloud status
			status == "RUNNABLE"
}

func IsReady(status string) bool {
	return status == "available" ||
		status == "configuring-enhanced-monitoring" ||
		status == "storage-optimization" ||
		status == "backing-up" ||
		// gcloud states
		status == "RUNNABLE" ||
		status == "UNKNOWN_STATE"
}

func InProgress(status string) bool {
	return status == "creating" || status == "starting" || status == "modifying" ||
		status == "rebooting" || status == "moving-to-vpc" ||
		status == "renaming" || status == "upgrading" || status == "backtracking" ||
		status == "maintenance" || status == "resetting-master-credentials" ||
		status == "rebooting cluster nodes" ||
		// gcloud states
		status == "PENDING_CREATE" || status == "MAINTENANCE"
}

func CanGetBindings(status string) bool {
	// Should we potentially add upgrading to this list?
	return  status != "creating" && status != "starting" && status != "modifying" &&
			status != "stopping" && status != "stopped" && status != "deleting" && status != "deleted" &&
			status != "incompatible-network" &&
			// gcloud states
			status != "SUSPENDED" && status != "PENDING_CREATE" && status != "MAINTENANCE" &&
			status != "FAILED" && status != "UNKNOWN_STATE"
}

func CanBeModified(status string) bool {
	// aws states
	return status != "creating" && status != "starting" && status != "modifying" &&
		status != "rebooting" && status != "moving-to-vpc" && status != "backing-up" &&
		status != "renaming" && status != "upgrading" && status != "backtracking" &&
		status != "maintenance" && status != "resetting-master-credentials" &&
		status != "deleted" && status != "rebooting cluster nodes" &&
		// gcloud states
		status != "SUSPENDED" && status != "PENDING_CREATE" && status != "MAINTENANCE" &&
		status != "FAILED" && status != "UNKNOWN_STATE"
}

//available, failed, incompatible-parameters, incompatible-network, restore-failed, recovering
func CanBeDeleted(status string) bool {
	return 	status == "available" || status == "failed" || status == "incompatible-parameters" || 
			status == "incompatible-network" || status == "restore-failed" || status == "recovering"
}