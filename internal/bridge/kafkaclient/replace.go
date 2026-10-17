package kafkaclient

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// keyRewrite is a topic/key rewrite (topicKeyRegex + topicKeyReplacement)
// with the replacement syntax of Core's Regex.replace: $n with the longest
// group number that exists, ${name}, \x for a literal x. A replacement Core
// rejects is rejected here when the device starts.
type keyRewrite struct {
	re    *regexp.Regexp
	parts []replPart
}

// replPart is a literal (group < 0) or a capture group reference.
type replPart struct {
	literal string
	group   int
}

func newKeyRewrite(pattern, replacement string) (*keyRewrite, error) {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}
	parts, err := parseReplacement(re, replacement)
	if err != nil {
		return nil, err
	}
	return &keyRewrite{re: re, parts: parts}, nil
}

func parseReplacement(re *regexp.Regexp, s string) ([]replPart, error) {
	groups := re.NumSubexp()
	var parts []replPart
	var lit strings.Builder
	flush := func() {
		if lit.Len() > 0 {
			parts = append(parts, replPart{literal: lit.String(), group: -1})
			lit.Reset()
		}
	}
	for i := 0; i < len(s); i++ {
		switch c := s[i]; c {
		case '\\':
			i++
			if i == len(s) {
				return nil, errors.New("character to be escaped is missing")
			}
			lit.WriteByte(s[i])
		case '$':
			i++
			if i == len(s) {
				return nil, errors.New("illegal group reference: group index is missing")
			}
			if s[i] == '{' {
				end := strings.IndexByte(s[i:], '}')
				if end < 0 {
					return nil, errors.New("named capturing group is missing trailing '}'")
				}
				name := s[i+1 : i+end]
				if !validGroupName(name) {
					return nil, fmt.Errorf("named capturing group %q is not a valid name", name)
				}
				idx := re.SubexpIndex(name)
				if idx < 0 {
					return nil, fmt.Errorf("no group with name {%s}", name)
				}
				flush()
				parts = append(parts, replPart{group: idx})
				i += end
				continue
			}
			if s[i] < '0' || s[i] > '9' {
				return nil, errors.New("illegal group reference")
			}
			ref := int(s[i] - '0')
			if ref > groups {
				return nil, fmt.Errorf("no group %d", ref)
			}
			for i+1 < len(s) && s[i+1] >= '0' && s[i+1] <= '9' {
				next := ref*10 + int(s[i+1]-'0')
				if next > groups {
					break
				}
				ref = next
				i++
			}
			flush()
			parts = append(parts, replPart{group: ref})
		default:
			lit.WriteByte(c)
		}
	}
	flush()
	return parts, nil
}

func validGroupName(name string) bool {
	if name == "" {
		return false
	}
	for i, r := range name {
		letter := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')
		if !letter && (i == 0 || r < '0' || r > '9') {
			return false
		}
	}
	return true
}

// apply replaces every match. An unmatched group contributes nothing.
func (k *keyRewrite) apply(s string) string {
	matches := k.re.FindAllStringSubmatchIndex(s, -1)
	if matches == nil {
		return s
	}
	var b strings.Builder
	last := 0
	for _, m := range matches {
		b.WriteString(s[last:m[0]])
		for _, p := range k.parts {
			if p.group < 0 {
				b.WriteString(p.literal)
			} else if m[2*p.group] >= 0 {
				b.WriteString(s[m[2*p.group]:m[2*p.group+1]])
			}
		}
		last = m[1]
	}
	b.WriteString(s[last:])
	return b.String()
}
