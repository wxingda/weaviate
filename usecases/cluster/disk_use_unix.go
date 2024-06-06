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

//go:build !windows

package cluster

import (
	"fmt"
	"runtime"
	"syscall"
)

// diskSpace return the disk space usage
func diskSpace(path string) (DiskUsage, error) {
	fs := syscall.Statfs_t{}
	err := syscall.Statfs(path, &fs)
	if err != nil {
		return DiskUsage{}, err
	}
	diskTotal := fs.Blocks * uint64(fs.Bsize)
	diskAvailable := fs.Bavail * uint64(fs.Bsize)
	diskPercentageUsed := float64(diskAvailable) / float64(diskTotal)
	// TODO gosigar, /proc/meminfo
	memStats := runtime.MemStats{}
	runtime.ReadMemStats(&memStats)
	// cpu usage https://github.com/shirou/gopsutil, https://github.com/mackerelio/go-osstat, /proc/stat
	// file descriptor usage /proc/<PID>/fd, lsof, https://github.com/influxdata/telegraf/pull/2609/files
	fmt.Println("NATEE usecases/cluster.diskSpace", diskTotal, diskAvailable, diskPercentageUsed, memStats.Alloc)
	return DiskUsage{
		Total:     diskTotal,
		Available: diskAvailable,
	}, nil
}
