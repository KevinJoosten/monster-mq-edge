package redisserver

import (
	"regexp"
	"strings"
)

// redisPatternToMqtt converts a Redis key pattern to an MQTT subscription filter
// and an optional post-filter function (F1).
func redisPatternToMqtt(pattern string) (string, func(string) bool) {
	if pattern == "*" {
		return "#", nil
	}
	if strings.ContainsAny(pattern, "+#") {
		return pattern, nil
	}
	if !hasUnescapedMeta(pattern) {
		return unescape(pattern), nil
	}

	segments := strings.Split(pattern, "/")
	segmentsWithMeta := make([]bool, len(segments))
	allMetaExactStar := true

	for i, seg := range segments {
		if hasUnescapedMeta(seg) {
			segmentsWithMeta[i] = true
			if seg != "*" {
				allMetaExactStar = false
			}
		}
	}

	if allMetaExactStar {
		newSegments := make([]string, len(segments))
		for i, seg := range segments {
			if seg == "*" {
				if i == len(segments)-1 {
					newSegments[i] = "#"
				} else {
					newSegments[i] = "+"
				}
			} else {
				newSegments[i] = unescape(seg)
			}
		}
		filter := strings.Join(newSegments, "/")
		if segments[len(segments)-1] == "*" {
			minLevels := len(segments)
			post := func(topic string) bool {
				return len(strings.Split(topic, "/")) >= minLevels
			}
			return filter, post
		}
		return filter, nil
	}

	firstMeta := 0
	for i, has := range segmentsWithMeta {
		if has {
			firstMeta = i
			break
		}
	}

	var prefixParts []string
	for _, seg := range segments[:firstMeta] {
		prefixParts = append(prefixParts, unescape(seg))
	}
	prefix := strings.Join(prefixParts, "/")

	var filter string
	if prefix == "" {
		filter = "#"
	} else {
		filter = prefix + "/#"
	}

	post := func(topic string) bool {
		return globMatch(pattern, topic)
	}
	return filter, post
}

func hasUnescapedMeta(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' {
			i++
			continue
		}
		if s[i] == '*' || s[i] == '?' || s[i] == '[' {
			return true
		}
	}
	return false
}

func unescape(s string) string {
	var sb strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) {
			i++
			sb.WriteByte(s[i])
		} else {
			sb.WriteByte(s[i])
		}
	}
	return sb.String()
}

func globMatch(pattern, text string) bool {
	re := globToRegex(pattern)
	return re.MatchString(text)
}

func globToRegex(pattern string) *regexp.Regexp {
	var sb strings.Builder
	sb.WriteString("^")
	n := len(pattern)
	for i := 0; i < n; i++ {
		c := pattern[i]
		switch c {
		case '\\':
			i++
			if i < n {
				sb.WriteString(regexp.QuoteMeta(string(pattern[i])))
			} else {
				sb.WriteString(regexp.QuoteMeta("\\"))
			}
		case '*':
			sb.WriteString(".*")
		case '?':
			sb.WriteString(".")
		case '[':
			j := i + 1
			if j < n && pattern[j] == '^' {
				j++
			}
			if j < n && pattern[j] == ']' {
				j++
			}
			for j < n && pattern[j] != ']' {
				if pattern[j] == '\\' && j+1 < n {
					j++
				}
				j++
			}
			if j < n && pattern[j] == ']' {
				inner := pattern[i+1 : j]
				if strings.HasPrefix(inner, "^") {
					sb.WriteString("[^")
					escapeCharClass(inner[1:], &sb)
					sb.WriteString("]")
				} else {
					sb.WriteString("[")
					escapeCharClass(inner, &sb)
					sb.WriteString("]")
				}
				i = j
			} else {
				sb.WriteString(regexp.QuoteMeta("["))
			}
		default:
			sb.WriteString(regexp.QuoteMeta(string(c)))
		}
	}
	sb.WriteString("$")
	r, err := regexp.Compile(sb.String())
	if err != nil {
		// Fallback to exact match on invalid regex
		return regexp.MustCompile("^" + regexp.QuoteMeta(pattern) + "$")
	}
	return r
}

func escapeCharClass(inner string, sb *strings.Builder) {
	k := 0
	for k < len(inner) {
		ch := inner[k]
		if ch == '\\' && k+1 < len(inner) {
			sb.WriteByte('\\')
			sb.WriteByte(inner[k+1])
			k += 2
		} else if ch == '-' && k > 0 && k+1 < len(inner) {
			sb.WriteByte('-')
			k++
		} else if ch == '[' || ch == ']' || ch == '-' || ch == '&' {
			sb.WriteByte('\\')
			sb.WriteByte(ch)
			k++
		} else {
			sb.WriteByte(ch)
			k++
		}
	}
}
