//go:build !linux

package rtspcamera

import (
	"context"
	"fmt"
	"runtime"
)

func (c *Connector) connectV4L2Stream(context.Context) error {
	return fmt.Errorf("v4l2:// USB cameras are only supported on Linux (running on %s); expose the camera as RTSP or HTTP MJPEG instead", runtime.GOOS)
}
