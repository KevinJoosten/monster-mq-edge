package unsmodeler

import (
	"archive/zip"
	"bytes"
	"compress/zlib"
	"fmt"
	"strings"
	"testing"
)

func buildPDF(t *testing.T, content string, flate bool, toUnicode string) []byte {
	t.Helper()
	var objs []string
	stream := []byte(content)
	filter := ""
	if flate {
		var z bytes.Buffer
		w := zlib.NewWriter(&z)
		w.Write(stream)
		w.Close()
		stream = z.Bytes()
		filter = " /Filter /FlateDecode"
	}
	fontObj := "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>"
	if toUnicode != "" {
		fontObj = "<< /Type /Font /Subtype /Type0 /BaseFont /X /ToUnicode 6 0 R >>"
	}
	objs = append(objs,
		"<< /Type /Catalog /Pages 2 0 R >>",
		"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
		"<< /Type /Page /Parent 2 0 R /Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>",
		fmt.Sprintf("<< /Length %d%s >>\nstream\n%s\nendstream", len(stream), filter, stream),
		fontObj)
	if toUnicode != "" {
		objs = append(objs, fmt.Sprintf("<< /Length %d >>\nstream\n%s\nendstream", len(toUnicode), toUnicode))
	}
	var b bytes.Buffer
	b.WriteString("%PDF-1.4\n")
	for i, o := range objs {
		fmt.Fprintf(&b, "%d 0 obj\n%s\nendobj\n", i+1, o)
	}
	b.WriteString("trailer << /Root 1 0 R >>\n%%EOF\n")
	return b.Bytes()
}

func TestExtractPDF(t *testing.T) {
	pdf := buildPDF(t, "BT /F1 12 Tf 72 720 Td (Plant Vienna \\(Hall 2\\)) Tj 0 -14 Td [(Pump) -300 (P101)] TJ ET", true, "")
	got, err := ExtractDocumentText("plant.pdf", pdf)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "Plant Vienna (Hall 2)") || !strings.Contains(got, "Pump P101") {
		t.Fatalf("text %q", got)
	}
	cmap := "begincmap\n1 begincodespacerange <0000> <FFFF> endcodespacerange\n2 beginbfchar\n<0001> <0048>\n<0002> <0069>\nendbfchar\n1 beginbfrange\n<0010> <0012> <0041>\nendbfrange\nendcmap"
	pdf = buildPDF(t, "BT /F1 12 Tf <00010002> Tj ( ) Tj <001000110012> Tj ET", false, cmap)
	got, err = ExtractDocumentText("x.pdf", pdf)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "Hi") || !strings.Contains(got, "ABC") {
		t.Fatalf("cmap text %q", got)
	}
	if _, err := ExtractDocumentText("x.pdf", []byte("%PDF-1.4\nnothing")); err == nil {
		t.Fatal("empty PDF accepted")
	}
}

func TestExtractDOCX(t *testing.T) {
	var b bytes.Buffer
	z := zip.NewWriter(&b)
	w, _ := z.Create("word/document.xml")
	w.Write([]byte(`<?xml version="1.0"?><w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"><w:body>
		<w:p><w:r><w:t>Filling line 1</w:t></w:r></w:p><w:p><w:r><w:t xml:space="preserve">Pump </w:t></w:r><w:r><w:t>P101</w:t></w:r></w:p></w:body></w:document>`))
	z.Close()
	got, err := ExtractDocumentText("plant.docx", b.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if got != "Filling line 1\nPump P101" {
		t.Fatalf("text %q", got)
	}
	if txt, _ := ExtractDocumentText("a.md", []byte("# Plant")); txt != "# Plant" {
		t.Fatal(txt)
	}
	if _, err := ExtractDocumentText("a.bin", []byte{0xff, 0xfe, 0x00}); err == nil {
		t.Fatal("binary accepted")
	}
}
