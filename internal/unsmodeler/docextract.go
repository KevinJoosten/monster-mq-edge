package unsmodeler

import (
	"archive/zip"
	"bytes"
	"compress/zlib"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"
)

// Context documents of AI runs (§4.2): text, Markdown, PDF and DOCX. The
// broker extracts the text itself, so every provider gets plain text.
const (
	MaxContextUploadBytes = 8 << 20 // raw upload
	maxPDFStreams         = 4000
)

// ExtractDocumentText returns the plain text of a context document. The
// format is taken from the file name and, for PDF and DOCX, the content.
func ExtractDocumentText(name string, data []byte) (string, error) {
	if len(data) > MaxContextUploadBytes {
		return "", fmt.Errorf("document has %d bytes, more than the upload limit of %d", len(data), MaxContextUploadBytes)
	}
	lower := strings.ToLower(name)
	switch {
	case bytes.HasPrefix(data, []byte("%PDF")) || strings.HasSuffix(lower, ".pdf"):
		return extractPDF(data)
	case bytes.HasPrefix(data, []byte("PK\x03\x04")) || strings.HasSuffix(lower, ".docx"):
		return extractDOCX(data)
	}
	if !utf8.Valid(data) {
		return "", errors.New("the document is not UTF-8 text, PDF or DOCX")
	}
	return string(data), nil
}

// extractDOCX reads the paragraphs of word/document.xml.
func extractDOCX(data []byte) (string, error) {
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return "", fmt.Errorf("DOCX: %w", err)
	}
	var doc *zip.File
	for _, f := range zr.File {
		if f.Name == "word/document.xml" {
			doc = f
		}
	}
	if doc == nil {
		return "", errors.New("DOCX: word/document.xml not found")
	}
	rc, err := doc.Open()
	if err != nil {
		return "", err
	}
	defer rc.Close()
	dec := xml.NewDecoder(io.LimitReader(rc, 64<<20))
	var b strings.Builder
	inText := false
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", fmt.Errorf("DOCX: %w", err)
		}
		switch t := tok.(type) {
		case xml.StartElement:
			switch t.Name.Local {
			case "t":
				inText = true
			case "tab":
				b.WriteByte('\t')
			case "br", "cr":
				b.WriteByte('\n')
			}
		case xml.EndElement:
			switch t.Name.Local {
			case "t":
				inText = false
			case "p":
				b.WriteByte('\n')
			case "tc":
				b.WriteByte('\t')
			}
		case xml.CharData:
			if inText {
				b.Write(t)
			}
		}
	}
	return strings.TrimSpace(b.String()), nil
}

var (
	pdfObjRe    = regexp.MustCompile(`(?s)(\d+)\s+(\d+)\s+obj(.*?)endobj`)
	pdfStreamRe = regexp.MustCompile(`(?s)^(.*?)stream\r?\n`)
	pdfRefRe    = regexp.MustCompile(`/ToUnicode\s+(\d+)\s+\d+\s+R`)
	pdfFontRe   = regexp.MustCompile(`/(F[\w.+-]*|[\w.+-]+)\s+(\d+)\s+\d+\s+R`)
)

type pdfObj struct {
	dict   string
	stream []byte
}

// extractPDF extracts the text shown by the content streams of a PDF: the
// strings of Tj, TJ, ' and " operators, decoded with the fonts' ToUnicode
// maps where present. Scanned (image-only) PDFs have no text.
func extractPDF(data []byte) (string, error) {
	objs := map[int]*pdfObj{}
	for i, m := range pdfObjRe.FindAllSubmatchIndex(data, -1) {
		if i > maxPDFStreams*4 {
			break
		}
		num, _ := strconv.Atoi(string(data[m[2]:m[3]]))
		body := data[m[6]:m[7]]
		o := &pdfObj{dict: string(body)}
		if sm := pdfStreamRe.FindSubmatchIndex(body); sm != nil {
			o.dict = string(body[sm[2]:sm[3]])
			raw := body[sm[1]:]
			if j := bytes.LastIndex(raw, []byte("endstream")); j >= 0 {
				raw = raw[:j]
			}
			o.stream = decodePDFStream(o.dict, raw)
		}
		objs[num] = o
	}
	if len(objs) == 0 {
		return "", errors.New("PDF: no objects found (encrypted or compressed object streams are not supported)")
	}
	// ToUnicode maps per font object.
	cmaps := map[string]map[string]string{}
	for num, o := range objs {
		if m := pdfRefRe.FindStringSubmatch(o.dict); m != nil {
			ref, _ := strconv.Atoi(m[1])
			if cm := objs[ref]; cm != nil && cm.stream != nil {
				cmaps[strconv.Itoa(num)] = parseCMap(cm.stream)
			}
		}
	}
	// Font resource names to font objects (merged over all pages).
	fonts := map[string]map[string]string{}
	for _, o := range objs {
		for _, m := range pdfFontRe.FindAllStringSubmatch(o.dict, -1) {
			if cm, ok := cmaps[m[2]]; ok {
				fonts[m[1]] = cm
			}
		}
	}
	var b strings.Builder
	for _, o := range objs {
		if o.stream == nil || strings.Contains(o.dict, "/Subtype /Image") || strings.Contains(o.dict, "/Subtype/Image") ||
			strings.Contains(o.dict, "/Type /XRef") || strings.Contains(o.dict, "/Type/XRef") || pdfRefRe.MatchString(o.dict) {
			continue
		}
		if !bytes.Contains(o.stream, []byte("BT")) {
			continue
		}
		b.WriteString(pdfContentText(o.stream, fonts))
		b.WriteString("\n")
	}
	text := strings.TrimSpace(b.String())
	if text == "" {
		return "", errors.New("PDF: no text found (scanned documents need OCR)")
	}
	return text, nil
}

func decodePDFStream(dict string, raw []byte) []byte {
	if strings.Contains(dict, "/FlateDecode") {
		zr, err := zlib.NewReader(bytes.NewReader(raw))
		if err != nil {
			return nil
		}
		out, _ := io.ReadAll(io.LimitReader(zr, 32<<20))
		return out
	}
	if strings.Contains(dict, "/Filter") {
		return nil
	}
	return raw
}

var (
	cmapBfcharRe  = regexp.MustCompile(`(?s)beginbfchar(.*?)endbfchar`)
	cmapBfrangeRe = regexp.MustCompile(`(?s)beginbfrange(.*?)endbfrange`)
	cmapHexRe     = regexp.MustCompile(`<([0-9A-Fa-f]*)>`)
)

func utf16Hex(h string) string {
	b, err := hex.DecodeString(h)
	if err != nil || len(b)%2 != 0 {
		return ""
	}
	u := make([]uint16, len(b)/2)
	for i := range u {
		u[i] = uint16(b[2*i])<<8 | uint16(b[2*i+1])
	}
	return string(utf16.Decode(u))
}

// parseCMap reads bfchar and bfrange entries of a ToUnicode CMap; keys are
// upper-case hex codes.
func parseCMap(s []byte) map[string]string {
	out := map[string]string{}
	defer func() {
		counts := map[int]int{}
		for k := range out {
			counts[len(k)]++
		}
		best := 4
		for l, n := range counts {
			if n > counts[best] {
				best = l
			}
		}
		out[cmapWidthKey] = strings.Repeat("x", best)
	}()
	for _, m := range cmapBfcharRe.FindAllSubmatch(s, -1) {
		hs := cmapHexRe.FindAllSubmatch(m[1], -1)
		for i := 0; i+1 < len(hs); i += 2 {
			out[strings.ToUpper(string(hs[i][1]))] = utf16Hex(string(hs[i+1][1]))
		}
	}
	for _, m := range cmapBfrangeRe.FindAllSubmatch(s, -1) {
		hs := cmapHexRe.FindAllSubmatch(m[1], -1)
		for i := 0; i+2 < len(hs); i += 3 {
			lo, err1 := strconv.ParseUint(string(hs[i][1]), 16, 32)
			hi, err2 := strconv.ParseUint(string(hs[i+1][1]), 16, 32)
			start := string(hs[i+2][1])
			if err1 != nil || err2 != nil || hi < lo || hi-lo > 0xffff {
				continue
			}
			base := []rune(utf16Hex(start))
			width := len(hs[i][1])
			for c := lo; c <= hi; c++ {
				if len(base) == 0 {
					break
				}
				r := base[len(base)-1] + rune(c-lo)
				out[fmt.Sprintf("%0*X", width, c)] = string(base[:len(base)-1]) + string(r)
			}
		}
	}
	return out
}

// cmapWidth is the hex width of the codes of a ToUnicode map.
func cmapWidth(cmap map[string]string) int {
	if w, ok := cmap[cmapWidthKey]; ok {
		return len(w)
	}
	return 4
}

// cmapWidthKey stores the code width (as a string of that length).
const cmapWidthKey = "\x00width"

// pdfContentText interprets the text operators of a content stream.
func pdfContentText(s []byte, fonts map[string]map[string]string) string {
	var b strings.Builder
	var cmap map[string]string
	var operands []string
	i := 0
	next := func() {
		for i < len(s) && (s[i] == ' ' || s[i] == '\n' || s[i] == '\r' || s[i] == '\t' || s[i] == '\f') {
			i++
		}
	}
	showHex := func(h string) {
		h = strings.ToUpper(h)
		if cmap != nil {
			width := cmapWidth(cmap)
			for k := 0; k+width <= len(h); k += width {
				if v, ok := cmap[h[k:k+width]]; ok {
					b.WriteString(v)
				}
			}
			return
		}
		raw, _ := hex.DecodeString(h)
		b.WriteString(latin1(raw))
	}
	showLiteral := func(raw []byte) {
		if cmap != nil {
			h := strings.ToUpper(hex.EncodeToString(raw))
			showHex(h)
			return
		}
		b.WriteString(latin1(raw))
	}
	for i < len(s) {
		next()
		if i >= len(s) {
			break
		}
		c := s[i]
		switch {
		case c == '(':
			raw, n := pdfLiteral(s[i:])
			i += n
			operands = append(operands, "(")
			showLiteral(raw)
		case c == '<' && i+1 < len(s) && s[i+1] != '<':
			j := bytes.IndexByte(s[i:], '>')
			if j < 0 {
				return b.String()
			}
			showHex(strings.Map(func(r rune) rune {
				if strings.ContainsRune("0123456789abcdefABCDEF", r) {
					return r
				}
				return -1
			}, string(s[i+1:i+j])))
			i += j + 1
			operands = append(operands, "<")
		case c == '[' || c == ']':
			i++
		case c == '/':
			j := i + 1
			for j < len(s) && !strings.ContainsRune(" \n\r\t/[]()<>", rune(s[j])) {
				j++
			}
			operands = append(operands, string(s[i:j]))
			i = j
		case c == '%':
			for i < len(s) && s[i] != '\n' {
				i++
			}
		default:
			j := i
			for j < len(s) && !strings.ContainsRune(" \n\r\t/[]()<>", rune(s[j])) {
				j++
			}
			if j == i {
				i++
				continue
			}
			tok := string(s[i:j])
			i = j
			switch tok {
			case "Tf":
				for k := len(operands) - 1; k >= 0; k-- {
					if strings.HasPrefix(operands[k], "/") {
						cmap = fonts[operands[k][1:]]
						break
					}
				}
			case "Td", "TD", "T*", "'", "\"":
				b.WriteByte(' ')
			case "ET":
				b.WriteByte('\n')
			case "TJ", "Tj":
			default:
				if _, err := strconv.ParseFloat(tok, 64); err == nil {
					// A large negative kerning in TJ arrays separates words.
					if f, _ := strconv.ParseFloat(tok, 64); f < -200 {
						b.WriteByte(' ')
					}
					operands = append(operands, tok)
					continue
				}
			}
			operands = operands[:0]
		}
	}
	return b.String()
}

func latin1(b []byte) string {
	r := make([]rune, 0, len(b))
	for _, c := range b {
		r = append(r, rune(c))
	}
	return string(r)
}

// pdfLiteral reads a (literal string) with escapes and nested parentheses.
func pdfLiteral(s []byte) ([]byte, int) {
	var out []byte
	depth := 0
	i := 0
	for i < len(s) {
		c := s[i]
		switch {
		case c == '(':
			if depth > 0 {
				out = append(out, c)
			}
			depth++
		case c == ')':
			depth--
			if depth == 0 {
				return out, i + 1
			}
			out = append(out, c)
		case c == '\\' && i+1 < len(s):
			i++
			e := s[i]
			switch e {
			case 'n':
				out = append(out, '\n')
			case 'r':
				out = append(out, '\r')
			case 't':
				out = append(out, '\t')
			case 'b', 'f':
			case '\r', '\n':
			default:
				if e >= '0' && e <= '7' {
					v := 0
					k := 0
					for k < 3 && i < len(s) && s[i] >= '0' && s[i] <= '7' {
						v = v*8 + int(s[i]-'0')
						i++
						k++
					}
					i--
					out = append(out, byte(v))
				} else {
					out = append(out, e)
				}
			}
		default:
			out = append(out, c)
		}
		i++
	}
	return out, i
}
