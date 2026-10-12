package rtspcamera

import (
	"bytes"
	"errors"
	"fmt"
	"image"
	"image/jpeg"
	"io"
	"net/url"
	"strconv"
	"strings"
)

// SchemeV4L2 selects a locally attached Video4Linux2 device (USB/UVC camera),
// e.g. v4l2:///dev/video0?width=1280&height=720&format=MJPEG&fps=15.
const SchemeV4L2 = "v4l2"

const (
	V4L2FormatAuto  = "AUTO"
	V4L2FormatMJPEG = "MJPEG"
	V4L2FormatYUYV  = "YUYV"
)

// v4l2Options is the device selection encoded in a v4l2:// URL.
type v4l2Options struct {
	Device string
	Width  int
	Height int
	Format string
	FPS    int
}

func parseV4L2URL(raw string) (v4l2Options, error) {
	opts := v4l2Options{Format: V4L2FormatAuto}
	u, err := url.Parse(raw)
	if err != nil {
		return opts, fmt.Errorf("invalid v4l2 url: %w", err)
	}
	if !strings.EqualFold(u.Scheme, SchemeV4L2) {
		return opts, fmt.Errorf("v4l2 url must use the v4l2:// scheme")
	}
	opts.Device = u.Path
	if u.Opaque != "" {
		opts.Device = u.Opaque
	}
	if u.Host != "" || !strings.HasPrefix(opts.Device, "/dev/") || len(opts.Device) <= len("/dev/") {
		return opts, fmt.Errorf("v4l2 url must name a device path, e.g. v4l2:///dev/video0")
	}
	q := u.Query()
	intParam := func(name string, max int) (int, error) {
		v := strings.TrimSpace(q.Get(name))
		if v == "" {
			return 0, nil
		}
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 || n > max {
			return 0, fmt.Errorf("v4l2 url parameter %s must be an integer between 1 and %d", name, max)
		}
		return n, nil
	}
	if opts.Width, err = intParam("width", 16384); err != nil {
		return opts, err
	}
	if opts.Height, err = intParam("height", 16384); err != nil {
		return opts, err
	}
	if (opts.Width == 0) != (opts.Height == 0) {
		return opts, fmt.Errorf("v4l2 url parameters width and height must be set together")
	}
	if opts.FPS, err = intParam("fps", 1000); err != nil {
		return opts, err
	}
	if f := strings.ToUpper(strings.TrimSpace(q.Get("format"))); f != "" {
		switch f {
		case V4L2FormatAuto, V4L2FormatMJPEG, V4L2FormatYUYV:
			opts.Format = f
		case "MJPG", "JPEG":
			opts.Format = V4L2FormatMJPEG
		case "YUY2":
			opts.Format = V4L2FormatYUYV
		default:
			return opts, fmt.Errorf("v4l2 url parameter format must be AUTO, MJPEG, or YUYV")
		}
	}
	return opts, nil
}

// yuyvFrame is one packed YUYV 4:2:2 picture copied out of a V4L2 buffer. It
// is immutable after creation and JPEG-encoded only when a snapshot is taken.
type yuyvFrame struct {
	width, height, stride int
	data                  []byte
}

func (f *yuyvFrame) WriteJPEG(w io.Writer, quality int) error {
	img, err := f.YCbCr()
	if err != nil {
		return err
	}
	return jpeg.Encode(w, img, &jpeg.Options{Quality: quality})
}

// YCbCr unpacks Y0 Cb Y1 Cr macropixels into planar 4:2:2 without color conversion.
func (f *yuyvFrame) YCbCr() (*image.YCbCr, error) {
	if f.width <= 0 || f.height <= 0 || f.width%2 != 0 || f.stride < f.width*2 || len(f.data) < f.stride*(f.height-1)+f.width*2 {
		return nil, errors.New("invalid YUYV frame geometry")
	}
	img := image.NewYCbCr(image.Rect(0, 0, f.width, f.height), image.YCbCrSubsampleRatio422)
	for y := 0; y < f.height; y++ {
		src := f.data[y*f.stride : y*f.stride+f.width*2]
		yRow := img.Y[y*img.YStride : y*img.YStride+f.width]
		cbRow := img.Cb[y*img.CStride : y*img.CStride+f.width/2]
		crRow := img.Cr[y*img.CStride : y*img.CStride+f.width/2]
		for x := 0; x < f.width/2; x++ {
			p := src[x*4 : x*4+4]
			yRow[2*x], cbRow[x], yRow[2*x+1], crRow[x] = p[0], p[1], p[2], p[3]
		}
	}
	return img, nil
}

// normalizeMJPEGFrame trims driver padding after EOI and inserts the standard
// Huffman tables that many UVC cameras omit (per the AVI1/MJPEG convention).
func normalizeMJPEGFrame(frame []byte) ([]byte, error) {
	if len(frame) < 4 || frame[0] != 0xff || frame[1] != 0xd8 {
		return nil, errors.New("MJPEG frame does not start with SOI")
	}
	end := bytes.LastIndex(frame, []byte{0xff, 0xd9})
	if end < 2 {
		return nil, errors.New("MJPEG frame has no EOI")
	}
	frame = frame[:end+2]

	// Walk marker segments up to SOS looking for a DHT.
	for i := 2; i+4 <= len(frame); {
		if frame[i] != 0xff {
			return nil, errors.New("MJPEG frame has a malformed marker segment")
		}
		marker := frame[i+1]
		switch {
		case marker == 0xff: // fill byte
			i++
			continue
		case marker == 0x01 || (marker >= 0xd0 && marker <= 0xd7): // standalone
			i += 2
			continue
		case marker == 0xc4:
			return frame, nil
		case marker == 0xda:
			out := make([]byte, 0, len(frame)+len(standardDHTSegment))
			out = append(out, frame[:i]...)
			out = append(out, standardDHTSegment...)
			return append(out, frame[i:]...), nil
		}
		i += 2 + (int(frame[i+2])<<8 | int(frame[i+3]))
	}
	return nil, errors.New("MJPEG frame has no SOS segment")
}

// standardDHTSegment holds the JPEG Annex K.3 tables (DC/AC luminance and chrominance).
var standardDHTSegment = buildDHTSegment([]huffmanSpec{
	{class: 0, id: 0, counts: [16]byte{0, 1, 5, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0},
		values: []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}},
	{class: 1, id: 0, counts: [16]byte{0, 2, 1, 3, 3, 2, 4, 3, 5, 5, 4, 4, 0, 0, 1, 0x7d},
		values: []byte{
			0x01, 0x02, 0x03, 0x00, 0x04, 0x11, 0x05, 0x12, 0x21, 0x31, 0x41, 0x06, 0x13, 0x51, 0x61, 0x07,
			0x22, 0x71, 0x14, 0x32, 0x81, 0x91, 0xa1, 0x08, 0x23, 0x42, 0xb1, 0xc1, 0x15, 0x52, 0xd1, 0xf0,
			0x24, 0x33, 0x62, 0x72, 0x82, 0x09, 0x0a, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x25, 0x26, 0x27, 0x28,
			0x29, 0x2a, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3a, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48, 0x49,
			0x4a, 0x53, 0x54, 0x55, 0x56, 0x57, 0x58, 0x59, 0x5a, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68, 0x69,
			0x6a, 0x73, 0x74, 0x75, 0x76, 0x77, 0x78, 0x79, 0x7a, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89,
			0x8a, 0x92, 0x93, 0x94, 0x95, 0x96, 0x97, 0x98, 0x99, 0x9a, 0xa2, 0xa3, 0xa4, 0xa5, 0xa6, 0xa7,
			0xa8, 0xa9, 0xaa, 0xb2, 0xb3, 0xb4, 0xb5, 0xb6, 0xb7, 0xb8, 0xb9, 0xba, 0xc2, 0xc3, 0xc4, 0xc5,
			0xc6, 0xc7, 0xc8, 0xc9, 0xca, 0xd2, 0xd3, 0xd4, 0xd5, 0xd6, 0xd7, 0xd8, 0xd9, 0xda, 0xe1, 0xe2,
			0xe3, 0xe4, 0xe5, 0xe6, 0xe7, 0xe8, 0xe9, 0xea, 0xf1, 0xf2, 0xf3, 0xf4, 0xf5, 0xf6, 0xf7, 0xf8,
			0xf9, 0xfa,
		}},
	{class: 0, id: 1, counts: [16]byte{0, 3, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0},
		values: []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}},
	{class: 1, id: 1, counts: [16]byte{0, 2, 1, 2, 4, 4, 3, 4, 7, 5, 4, 4, 0, 1, 2, 0x77},
		values: []byte{
			0x00, 0x01, 0x02, 0x03, 0x11, 0x04, 0x05, 0x21, 0x31, 0x06, 0x12, 0x41, 0x51, 0x07, 0x61, 0x71,
			0x13, 0x22, 0x32, 0x81, 0x08, 0x14, 0x42, 0x91, 0xa1, 0xb1, 0xc1, 0x09, 0x23, 0x33, 0x52, 0xf0,
			0x15, 0x62, 0x72, 0xd1, 0x0a, 0x16, 0x24, 0x34, 0xe1, 0x25, 0xf1, 0x17, 0x18, 0x19, 0x1a, 0x26,
			0x27, 0x28, 0x29, 0x2a, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3a, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48,
			0x49, 0x4a, 0x53, 0x54, 0x55, 0x56, 0x57, 0x58, 0x59, 0x5a, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68,
			0x69, 0x6a, 0x73, 0x74, 0x75, 0x76, 0x77, 0x78, 0x79, 0x7a, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87,
			0x88, 0x89, 0x8a, 0x92, 0x93, 0x94, 0x95, 0x96, 0x97, 0x98, 0x99, 0x9a, 0xa2, 0xa3, 0xa4, 0xa5,
			0xa6, 0xa7, 0xa8, 0xa9, 0xaa, 0xb2, 0xb3, 0xb4, 0xb5, 0xb6, 0xb7, 0xb8, 0xb9, 0xba, 0xc2, 0xc3,
			0xc4, 0xc5, 0xc6, 0xc7, 0xc8, 0xc9, 0xca, 0xd2, 0xd3, 0xd4, 0xd5, 0xd6, 0xd7, 0xd8, 0xd9, 0xda,
			0xe2, 0xe3, 0xe4, 0xe5, 0xe6, 0xe7, 0xe8, 0xe9, 0xea, 0xf2, 0xf3, 0xf4, 0xf5, 0xf6, 0xf7, 0xf8,
			0xf9, 0xfa,
		}},
})

type huffmanSpec struct {
	class, id byte
	counts    [16]byte
	values    []byte
}

func buildDHTSegment(specs []huffmanSpec) []byte {
	length := 2
	for _, s := range specs {
		length += 17 + len(s.values)
	}
	seg := []byte{0xff, 0xc4, byte(length >> 8), byte(length)}
	for _, s := range specs {
		seg = append(seg, s.class<<4|s.id)
		seg = append(seg, s.counts[:]...)
		seg = append(seg, s.values...)
	}
	return seg
}
