package api

import "time"

// TypeMeta says what kind of object this is
type TypeMeta struct {
	Kind       string `json:"kind"`
	APIVersion string `json:"apiVersion"`
}

// ObjectMeta is the identity and bookkeeping data every object carries
type ObjectMeta struct {
	Name              string            `json:"name"`
	Namespace         string            `json:"namespace"`
	UID               string            `json:"uid"`
	Labels            map[string]string `json:"labels,omitempty"` // in kubernetes, Labels is the optinal metaData. there is no reason to serialize an empty label map when the object doesnt has labels
	CreationTimestamp time.Time         `json:"creationTimestamp"`
}

type Container struct {
	Name  string `json:"name"`
	Image string `json"image"`
	Ports []int  `json:"ports,omitempty"`
}

// PodSpec is the DESIRED state (given by the user)
type PodSpec struct {
	Containers []Container `json:"containers"`
	NodeName   string      `json:"nodeName,omitempty"`
}

type PodPhase string

const (
	PodPending   PodPhase = "Pending"
	PodRunning   PodPhase = "Running"
	PodSucceeded PodPhase = "Succeeded"
	PodFailed    PodPhase = "Failed"
)

// PodStatus is the OBSERVED state (the data we get from the running system)
type PodStatus struct {
	Phase PodPhase `json:"phase"`
}

type Pod struct {
	TypeMeta
	Metadata ObjectMeta `json:"metadata"`
	Spec     PodSpec    `json:"spec"`
	Status   PodStatus  `json:"status"`
}

type PodList struct {
	TypeMeta
	Items []Pod `json:"items"`
}

// Embeddig typemeta w/o a field name makes its json fields appeaar at the top level
