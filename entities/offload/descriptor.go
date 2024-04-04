//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package offload

import (
	"fmt"
	"time"
)

// NodeDescriptor contains data related to one participant in offload
type NodeDescriptor struct {
	Status Status `json:"status"`
	Error  string `json:"error"`
}

type OffloadDistributedDescriptor struct {
	ID            string                     `json:"id"`
	Class         string                     `json:"class"`
	Tenant        string                     `json:"tenant"`
	StartedAt     time.Time                  `json:"startedAt"`
	CompletedAt   time.Time                  `json:"completedAt"`
	Status        Status                     `json:"status"`
	Nodes         map[string]*NodeDescriptor `json:"nodes"`
	NodeMapping   map[string]string          `json:"node_mapping"`
	Version       string                     `json:"version"`
	ServerVersion string                     `json:"serverVersion"`
	Error         string                     `json:"error"`
}

// // ToMappedNodeName will return nodeName after applying d.NodeMapping translation on it.
// // If nodeName is not contained in d.nodeMapping, returns nodeName unmodified
func (d *OffloadDistributedDescriptor) ToMappedNodeName(nodeName string) string {
	if newNodeName, ok := d.NodeMapping[nodeName]; ok {
		return newNodeName
	}
	return nodeName
}

// // ToOriginalNodeName will return nodeName after trying to find an original node name from d.NodeMapping values.
// // If nodeName is not contained in d.nodeMapping values, returns nodeName unmodified
func (d *OffloadDistributedDescriptor) ToOriginalNodeName(nodeName string) string {
	for oldNodeName, newNodeName := range d.NodeMapping {
		if newNodeName == nodeName {
			return oldNodeName
		}
	}
	return nodeName
}

// // ApplyNodeMapping applies d.NodeMapping translation to d.Nodes. If a node in d.Nodes is not translated by d.NodeMapping, it will remain
// // unchanged.
// func (d *DistributedBackupDescriptor) ApplyNodeMapping() {
// 	if len(d.NodeMapping) == 0 {
// 		return
// 	}

// 	for k, v := range d.NodeMapping {
// 		if nodeDescriptor, ok := d.Nodes[k]; !ok {
// 			d.Nodes[v] = nodeDescriptor
// 			delete(d.Nodes, k)
// 		}
// 	}
// }

func (d *OffloadDistributedDescriptor) Validate() error {
	if d.Error != "" {
		return fmt.Errorf("error present %q", d.Error)
	}
	if d.ID == "" {
		return fmt.Errorf("missing ID")
	}
	if d.Class == "" {
		return fmt.Errorf("missing Class")
	}
	if d.Tenant == "" {
		return fmt.Errorf("missing Tenant")
	}
	if d.StartedAt.IsZero() {
		return fmt.Errorf("missing StartedAt")
	}
	if d.CompletedAt.IsZero() {
		return fmt.Errorf("missing CompletedAt")
	}
	if d.Version == "" {
		return fmt.Errorf("missing Version")
	}
	if d.ServerVersion == "" {
		return fmt.Errorf("missing ServerVersion")
	}
	if len(d.Nodes) == 0 {
		return fmt.Errorf("empty list of node descriptors")
	}
	return nil
}

// // resetStatus sets status and sub-statuses to Started
// // It also empties error and sub-errors
func (d *OffloadDistributedDescriptor) ResetStatus() *OffloadDistributedDescriptor {
	d.Status = Started
	d.Error = ""
	d.StartedAt = time.Now()
	d.CompletedAt = time.Time{}
	for _, node := range d.Nodes {
		node.Status = Started
		node.Error = ""
	}
	return d
}

type ShardDescriptor struct {
	Tenant string   `json:"tenant"`
	Class  string   `json:"class"`
	Node   string   `json:"node"`
	Files  []string `json:"files,omitempty"`
	Error  error    `json:"-"`
}

type OffloadNodeDescriptor struct {
	ID            string    `json:"id"`
	Class         string    `json:"class"`
	Tenant        string    `json:"tenant"`
	Node          string    `json:"node"`
	Files         []string  `json:"files,omitempty"`
	StartedAt     time.Time `json:"startedAt"`
	CompletedAt   time.Time `json:"completedAt"`
	Status        string    `json:"status"` // "STARTED|TRANSFERRING|TRANSFERRED|SUCCESS|FAILED"
	Version       string    `json:"version"`
	ServerVersion string    `json:"serverVersion"`
	Error         string    `json:"error"`
}

func (d *OffloadNodeDescriptor) Validate() error {
	if d.Error != "" {
		return fmt.Errorf("error present %q", d.Error)
	}
	if d.ID == "" {
		return fmt.Errorf("missing ID")
	}
	if d.Class == "" {
		return fmt.Errorf("missing Class")
	}
	if d.Tenant == "" {
		return fmt.Errorf("missing Tenant")
	}
	if d.Node == "" {
		return fmt.Errorf("missing Node")
	}
	if d.StartedAt.IsZero() {
		return fmt.Errorf("missing StartedAt")
	}
	if d.CompletedAt.IsZero() {
		return fmt.Errorf("missing CompletedAt")
	}
	if d.Version == "" {
		return fmt.Errorf("missing Version")
	}
	if d.ServerVersion == "" {
		return fmt.Errorf("missing ServerVersion")
	}
	return nil
}
