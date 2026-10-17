package kafkaclient

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"unicode/utf8"
)

// jsonObject reads a JSON object with the value rules of Core's JSON layer,
// so a config or envelope that Core accepts or rejects behaves the same here:
//   - str: any value as its string form; absent and null give no value
//   - boolean / integer / long: absent or null give no value (the "def"
//     variants: absent gives the default, an explicit null is an error, as
//     the null fails in Core); a value of another type is an error
type jsonObject map[string]json.RawMessage

func parseObject(b []byte) (jsonObject, error) {
	var obj jsonObject
	if err := json.Unmarshal(b, &obj); err != nil {
		return nil, err
	}
	if obj == nil {
		return nil, fmt.Errorf("not a JSON object")
	}
	return obj, nil
}

func (o jsonObject) raw(key string) (json.RawMessage, bool) {
	r, ok := o[key]
	if !ok || isNull(r) {
		return nil, false
	}
	return r, true
}

func (o jsonObject) isExplicitNull(key string) bool {
	r, ok := o[key]
	return ok && isNull(r)
}

func isNull(r json.RawMessage) bool { return strings.TrimSpace(string(r)) == "null" }

// str is getString: any value converted to its string form.
func (o jsonObject) str(key string) (string, bool) {
	r, ok := o.raw(key)
	if !ok {
		return "", false
	}
	return valueString(r), true
}

func (o jsonObject) strPtr(key string) *string {
	if s, ok := o.str(key); ok {
		return &s
	}
	return nil
}

// boolean is getBoolean(key): no value when absent or null.
func (o jsonObject) boolean(key string) (bool, bool, error) {
	r, ok := o.raw(key)
	if !ok {
		return false, false, nil
	}
	var b bool
	if err := json.Unmarshal(r, &b); err != nil {
		return false, false, fmt.Errorf("%s: %s is not a boolean", key, r)
	}
	return b, true, nil
}

// booleanDef is getBoolean(key, def).
func (o jsonObject) booleanDef(key string, def bool) (bool, error) {
	if o.isExplicitNull(key) {
		return false, fmt.Errorf("%s: null", key)
	}
	b, ok, err := o.boolean(key)
	if err != nil || !ok {
		return def, err
	}
	return b, nil
}

// number reads a JSON number as Core's Number.longValue() would.
func (o jsonObject) number(key string) (int64, bool, error) {
	r, ok := o.raw(key)
	if !ok {
		return 0, false, nil
	}
	s := strings.TrimSpace(string(r))
	if s == "" || !(s[0] == '-' || (s[0] >= '0' && s[0] <= '9')) {
		return 0, false, fmt.Errorf("%s: %s is not a number", key, r)
	}
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return i, true, nil
	}
	if bi, ok := new(big.Int).SetString(s, 10); ok {
		return bi.Int64(), true, nil // wraps like longValue()
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, false, fmt.Errorf("%s: %s is not a number", key, r)
	}
	return int64(f), true, nil
}

// integerDef is getInteger(key, def): the long value narrowed like intValue().
func (o jsonObject) integerDef(key string, def int64) (int64, error) {
	if o.isExplicitNull(key) {
		return 0, fmt.Errorf("%s: null", key)
	}
	n, ok, err := o.number(key)
	if err != nil || !ok {
		return def, err
	}
	return int64(int32(n)), nil
}

// longDef is getLong(key, def).
func (o jsonObject) longDef(key string, def int64) (int64, error) {
	if o.isExplicitNull(key) {
		return 0, fmt.Errorf("%s: null", key)
	}
	n, ok, err := o.number(key)
	if err != nil || !ok {
		return def, err
	}
	return n, nil
}

// object is getJsonObject(key): no value when absent or null.
func (o jsonObject) object(key string) (jsonObject, bool, error) {
	r, ok := o.raw(key)
	if !ok {
		return nil, false, nil
	}
	obj, err := parseObject(r)
	if err != nil {
		return nil, false, fmt.Errorf("%s: %s is not an object", key, r)
	}
	return obj, true, nil
}

// array is getJsonArray(key): no value when absent or null.
func (o jsonObject) array(key string) ([]json.RawMessage, bool, error) {
	r, ok := o.raw(key)
	if !ok {
		return nil, false, nil
	}
	var items []json.RawMessage
	if err := json.Unmarshal(r, &items); err != nil {
		return nil, false, fmt.Errorf("%s: %s is not an array", key, r)
	}
	return items, true, nil
}

// stringMap converts every value with toString(), as Core does for the extra
// Kafka properties. A null value fails, as it does in Core.
func (o jsonObject) stringMap(key string) (map[string]string, bool, error) {
	obj, ok, err := o.object(key)
	if err != nil || !ok {
		return nil, ok, err
	}
	out := make(map[string]string, len(obj))
	for k, v := range obj {
		if isNull(v) {
			return nil, false, fmt.Errorf("%s.%s: null", key, k)
		}
		out[k] = valueString(v)
	}
	return out, true, nil
}

// valueString is a JSON value's toString() in Core: strings as is, integers
// in decimal, other numbers like Double.toString, true/false, and objects or
// arrays as compact JSON.
func valueString(r json.RawMessage) string {
	s := strings.TrimSpace(string(r))
	if s == "" {
		return ""
	}
	switch s[0] {
	case '"':
		var v string
		_ = json.Unmarshal(r, &v)
		return v
	case '{', '[':
		var buf bytes.Buffer
		if json.Compact(&buf, r) == nil {
			return buf.String()
		}
		return s
	case 't', 'f':
		return s
	case 'n':
		return "null"
	}
	if !strings.ContainsAny(s, ".eE") {
		if bi, ok := new(big.Int).SetString(s, 10); ok {
			return bi.String()
		}
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return DoubleString(f)
	}
	return s
}

// DoubleString formats f the way Core prints a double: plain notation with at least one
// fraction digit for 1e-3 <= |f| < 1e7, otherwise d.dddE[-]n.
func DoubleString(f float64) string {
	switch {
	case math.IsNaN(f):
		return "NaN"
	case math.IsInf(f, 1):
		return "Infinity"
	case math.IsInf(f, -1):
		return "-Infinity"
	case f == 0:
		if math.Signbit(f) {
			return "-0.0"
		}
		return "0.0"
	}
	abs := math.Abs(f)
	if abs >= 1e-3 && abs < 1e7 {
		s := strconv.FormatFloat(f, 'f', -1, 64)
		if !strings.Contains(s, ".") {
			s += ".0"
		}
		return s
	}
	e := strconv.FormatFloat(f, 'e', -1, 64) // e.g. 1.234e+07
	mant, exp, _ := strings.Cut(e, "e")
	if !strings.Contains(mant, ".") {
		mant += ".0"
	}
	n, _ := strconv.Atoi(exp)
	return mant + "E" + strconv.Itoa(n)
}

// decodeBase64 decodes like Core's basic Base64 decoder: padding is optional,
// line breaks and other non-alphabet characters are rejected.
func decodeBase64(s string) ([]byte, error) {
	if strings.ContainsAny(s, "\r\n") {
		return nil, fmt.Errorf("illegal base64 character")
	}
	return base64.RawStdEncoding.DecodeString(strings.TrimRight(s, "="))
}

// utf8String replaces each invalid UTF-8 byte with U+FFFD, as Core's string
// (de)serialisation of record keys and TEXT values does.
func utf8String(b []byte) []byte {
	if utf8.Valid(b) {
		return b
	}
	out := make([]byte, 0, len(b)+8)
	for len(b) > 0 {
		r, size := utf8.DecodeRune(b)
		if r == utf8.RuneError && size <= 1 {
			out = utf8.AppendRune(out, utf8.RuneError)
			b = b[1:]
			continue
		}
		out = append(out, b[:size]...)
		b = b[size:]
	}
	return out
}
