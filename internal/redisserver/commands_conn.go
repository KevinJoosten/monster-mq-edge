package redisserver

import (
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	"monstermq.io/edge/internal/version"
)

func (c *Conn) tryAuth(username, password string) bool {
	if username == "" {
		return false
	}
	if c.server.deps.Auth == nil {
		return false
	}
	if c.server.deps.Auth.Validate(username, password) {
		c.username = username
		c.authenticated = true
		c.registerSession()
		return true
	}
	if username == "default" {
		if c.server.deps.Auth.Validate("Admin", password) {
			c.username = "Admin"
			c.authenticated = true
			c.registerSession()
			return true
		}
	}
	return false
}

func (c *Conn) handleAuth(args [][]byte) error {
	if len(args) < 2 || len(args) > 3 {
		return c.writeError("ERR wrong number of arguments for 'auth' command")
	}

	var user, pass string
	if len(args) == 2 {
		user = "default"
		pass = string(args[1])
	} else {
		user = string(args[1])
		pass = string(args[2])
	}

	if c.tryAuth(user, pass) {
		return c.writeSimple("OK")
	}
	return c.writeError("WRONGPASS invalid username-password pair or user is disabled.")
}

func (c *Conn) handleHello(args [][]byte) error {
	if len(args) >= 2 {
		if proto, err := strconv.Atoi(string(args[1])); err == nil && proto > 2 {
			return c.writeError("NOPROTO sorry, this protocol version is not supported.")
		}
	}

	authIdx := -1
	for i, arg := range args {
		if strings.EqualFold(string(arg), "AUTH") {
			authIdx = i
			break
		}
	}

	if authIdx != -1 && authIdx+2 < len(args) {
		authUser := string(args[authIdx+1])
		authPass := string(args[authIdx+2])
		if c.tryAuth(authUser, authPass) {
			return c.writeHello()
		}
		return c.writeError("WRONGPASS invalid username-password pair or user is disabled.")
	}

	if c.authenticated {
		c.registerSession()
	}
	return c.writeHello()
}

func (c *Conn) writeHello() error {
	ver := version.Version
	if ver == "" {
		ver = "0.0.0"
	}
	items := []respValue{
		respBulk([]byte("server")),
		respBulk([]byte("monstermq")),
		respBulk([]byte("version")),
		respBulk([]byte(ver)),
		respBulk([]byte("proto")),
		respInteger(2),
		respBulk([]byte("id")),
		respInteger(c.id),
		respBulk([]byte("mode")),
		respBulk([]byte("standalone")),
		respBulk([]byte("role")),
		respBulk([]byte("master")),
		respBulk([]byte("modules")),
		respArray(nil), // empty array *0\r\n
	}
	return c.writeArray(items)
}

func (c *Conn) handlePing(args [][]byte) error {
	if len(args) > 2 {
		return c.writeError("ERR wrong number of arguments for 'ping' command")
	}
	if len(args) == 2 {
		return c.writeBulk(args[1])
	}
	return c.writeSimple("PONG")
}

func (c *Conn) handleEcho(args [][]byte) error {
	if len(args) != 2 {
		return c.writeError("ERR wrong number of arguments for 'echo' command")
	}
	return c.writeBulk(args[1])
}

func (c *Conn) handleQuit(args [][]byte) error {
	_ = c.writeSimple("OK")
	_ = c.flush()
	c.Close()
	return io.EOF
}

func (c *Conn) handleSelect(args [][]byte) error {
	if len(args) != 2 {
		return c.writeError("ERR wrong number of arguments for 'select' command")
	}
	db, err := strconv.Atoi(string(args[1]))
	if err != nil || db < 0 {
		return c.writeError("ERR invalid DB index")
	}
	if c.server.deps.Archives == nil || c.server.deps.Archives.ByRedisDB(db) == nil {
		return c.writeError("ERR DB index is not mapped to an archive group")
	}
	c.selectedDb = db
	c.updateSessionInfo()
	return c.writeSimple("OK")
}

func (c *Conn) handleClient(args [][]byte) error {
	if len(args) < 2 {
		return c.writeError("ERR wrong number of arguments for 'client' command")
	}
	subcmd := strings.ToUpper(string(args[1]))
	switch subcmd {
	case "SETNAME":
		if len(args) >= 3 {
			c.clientName = string(args[2])
		} else {
			c.clientName = ""
		}
		c.updateSessionInfo()
		return c.writeSimple("OK")
	case "GETNAME":
		if c.clientName != "" {
			return c.writeBulkStr(c.clientName)
		}
		return c.writeNullBulk()
	case "SETINFO":
		return c.writeSimple("OK")
	case "ID":
		return c.writeInteger(c.id)
	default:
		return c.writeError("ERR unsupported CLIENT subcommand")
	}
}

func (c *Conn) handleCommand(args [][]byte) error {
	if len(args) >= 2 && strings.EqualFold(string(args[1]), "COUNT") {
		return c.writeInteger(30)
	}
	if len(args) >= 2 && strings.EqualFold(string(args[1]), "INFO") {
		var items []respValue
		for i := 2; i < len(args); i++ {
			items = append(items, respBulk(nil))
		}
		return c.writeArray(items)
	}
	return c.writeArray(nil)
}

func (c *Conn) handleInfo(args [][]byte) error {
	var dbs []int
	if c.server.deps.Archives != nil {
		seen := make(map[int]bool)
		for _, g := range c.server.deps.Archives.Snapshot() {
			if eff := g.Config().EffectiveRedisDbNumber(); eff != nil {
				if !seen[*eff] {
					seen[*eff] = true
					dbs = append(dbs, *eff)
				}
			}
		}
		sort.Ints(dbs)
	}

	ver := version.Version
	if ver == "" {
		ver = "0.0.0"
	}

	var sb strings.Builder
	sb.WriteString("# Server\r\n")
	sb.WriteString("redis_version:7.0.0\r\n")
	sb.WriteString(fmt.Sprintf("monstermq_version:%s\r\n", ver))
	sb.WriteString("redis_mode:standalone\r\n")
	sb.WriteString("# Clients\r\n")
	sb.WriteString("connected_clients:1\r\n")
	sb.WriteString("# Keyspace\r\n")
	for _, db := range dbs {
		sb.WriteString(fmt.Sprintf("db%d:keys=0,expires=0,avg_ttl=0\r\n", db))
	}

	return c.writeBulk([]byte(sb.String()))
}
