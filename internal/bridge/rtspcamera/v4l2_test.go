package rtspcamera

import (
	"bytes"
	"image"
	"image/color"
	"image/jpeg"
	"strings"
	"testing"
)

func TestParseV4L2URL(t *testing.T) {
	opts, err := parseV4L2URL("v4l2:///dev/video0?width=1280&height=720&format=mjpg&fps=15")
	if err != nil {
		t.Fatal(err)
	}
	want := v4l2Options{Device: "/dev/video0", Width: 1280, Height: 720, Format: V4L2FormatMJPEG, FPS: 15}
	if opts != want {
		t.Fatalf("got %+v, want %+v", opts, want)
	}
	opts, err = parseV4L2URL("v4l2:/dev/v4l/by-id/usb-Logitech_C920-video-index0")
	if err != nil || opts.Device != "/dev/v4l/by-id/usb-Logitech_C920-video-index0" || opts.Format != V4L2FormatAuto {
		t.Fatalf("got %+v, %v", opts, err)
	}
	for _, bad := range []string{
		"v4l2://video0",
		"v4l2:///tmp/video0",
		"v4l2:///dev/",
		"v4l2:///dev/video0?width=640",
		"v4l2:///dev/video0?width=0&height=480",
		"v4l2:///dev/video0?format=H264",
		"v4l2:///dev/video0?fps=abc",
	} {
		if _, err := parseV4L2URL(bad); err == nil {
			t.Errorf("parseV4L2URL(%q) succeeded, want error", bad)
		}
	}
}

func TestConfigValidateAcceptsV4L2(t *testing.T) {
	cfg := DefaultConfig()
	cfg.URL = "v4l2:///dev/video0"
	if errs := cfg.Validate(); len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	cfg.URL = "v4l2:///dev/video0?format=bogus"
	if errs := cfg.Validate(); len(errs) != 1 || !strings.Contains(errs[0], "format") {
		t.Fatalf("expected format error, got %v", errs)
	}
}

func testJPEG(t *testing.T) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, 32, 16))
	for y := 0; y < 16; y++ {
		for x := 0; x < 32; x++ {
			img.Set(x, y, color.RGBA{uint8(x * 8), uint8(y * 16), uint8(x * y), 255})
		}
	}
	var buf bytes.Buffer
	if err := jpeg.Encode(&buf, img, &jpeg.Options{Quality: 90}); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// stripDHT removes all DHT segments, mimicking UVC MJPEG output.
func stripDHT(t *testing.T, src []byte) []byte {
	t.Helper()
	out := []byte{0xff, 0xd8}
	i := 2
	for {
		marker := src[i+1]
		if marker == 0xda {
			return append(out, src[i:]...)
		}
		n := 2 + (int(src[i+2])<<8 | int(src[i+3]))
		if marker != 0xc4 {
			out = append(out, src[i:i+n]...)
		}
		i += n
	}
}

func TestNormalizeMJPEGFrameInsertsStandardHuffmanTables(t *testing.T) {
	orig := testJPEG(t)
	want, err := jpeg.Decode(bytes.NewReader(orig))
	if err != nil {
		t.Fatal(err)
	}
	stripped := stripDHT(t, orig)
	if _, err := jpeg.Decode(bytes.NewReader(stripped)); err == nil {
		t.Fatal("stripped JPEG unexpectedly decodes")
	}
	// UVC drivers report bytesused for the whole buffer, so add padding after EOI.
	padded := append(append([]byte(nil), stripped...), make([]byte, 64)...)
	fixed, err := normalizeMJPEGFrame(padded)
	if err != nil {
		t.Fatal(err)
	}
	if fixed[len(fixed)-2] != 0xff || fixed[len(fixed)-1] != 0xd9 {
		t.Fatal("padding after EOI was not trimmed")
	}
	got, err := jpeg.Decode(bytes.NewReader(fixed))
	if err != nil {
		t.Fatalf("decode repaired frame: %v", err)
	}
	// Go's encoder uses the same Annex K tables, so the result must be identical.
	if !bytes.Equal(got.(*image.YCbCr).Y, want.(*image.YCbCr).Y) {
		t.Fatal("repaired frame decodes to different pixels")
	}

	same, err := normalizeMJPEGFrame(orig)
	if err != nil || !bytes.Equal(same, orig) {
		t.Fatalf("frame with DHT should pass through unchanged (err=%v)", err)
	}
	for _, bad := range [][]byte{nil, {0x00, 0x01, 0x02, 0x03}, orig[:len(orig)/2]} {
		if _, err := normalizeMJPEGFrame(bad); err == nil {
			t.Error("expected error for invalid frame")
		}
	}
}

func TestYUYVFrameWriteJPEG(t *testing.T) {
	const w, h, stride = 4, 2, 10 // stride includes 2 bytes of row padding
	data := []byte{
		10, 100, 20, 200, 30, 110, 40, 210, 0, 0,
		50, 120, 60, 220, 70, 130, 80, 230, 0, 0,
	}
	f := &yuyvFrame{width: w, height: h, stride: stride, data: data}
	img, err := f.YCbCr()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(img.Y[:4], []byte{10, 20, 30, 40}) || img.Cb[img.CStride] != 120 || img.Cr[img.CStride+1] != 230 {
		t.Fatalf("unexpected planes Y=%v Cb=%v Cr=%v", img.Y, img.Cb, img.Cr)
	}
	var buf bytes.Buffer
	if err := f.WriteJPEG(&buf, 85); err != nil {
		t.Fatal(err)
	}
	cfg, err := jpeg.DecodeConfig(&buf)
	if err != nil || cfg.Width != w || cfg.Height != h {
		t.Fatalf("got %+v, %v", cfg, err)
	}
	if _, err := (&yuyvFrame{width: 3, height: 1, stride: 6, data: make([]byte, 6)}).YCbCr(); err == nil {
		t.Fatal("expected error for odd width")
	}
}
