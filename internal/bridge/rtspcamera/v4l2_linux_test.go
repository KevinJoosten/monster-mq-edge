//go:build linux

package rtspcamera

import (
	"testing"
	"unsafe"
)

// Sizes from <linux/videodev2.h>; they are encoded into the ioctl numbers.
func TestV4L2StructSizesMatchKernelABI(t *testing.T) {
	is64 := unsafe.Sizeof(uintptr(0)) == 8
	pick := func(a, b uintptr) uintptr {
		if is64 {
			return a
		}
		return b
	}
	cases := []struct {
		name      string
		got, want uintptr
	}{
		{"v4l2_capability", unsafe.Sizeof(v4l2Capability{}), 104},
		{"v4l2_pix_format", unsafe.Sizeof(v4l2PixFormat{}), 48},
		{"v4l2_format", unsafe.Sizeof(v4l2Format{}), pick(208, 204)},
		{"v4l2_requestbuffers", unsafe.Sizeof(v4l2RequestBuffers{}), 20},
		{"v4l2_buffer", unsafe.Sizeof(v4l2Buffer{}), pick(88, 68)},
		{"v4l2_streamparm", unsafe.Sizeof(v4l2StreamParm{}), 204},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s: size %d, want %d", c.name, c.got, c.want)
		}
	}
	if is64 && vidiocDQBuf != 0xc0585611 {
		t.Errorf("VIDIOC_DQBUF = %#x, want 0xc0585611", vidiocDQBuf)
	}
	if !is64 && vidiocDQBuf != 0xc0445611 {
		t.Errorf("VIDIOC_DQBUF = %#x, want 0xc0445611", vidiocDQBuf)
	}
}
