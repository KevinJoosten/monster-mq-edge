package broker

import (
	"monstermq.io/edge/internal/mqtt/packets"
	"monstermq.io/edge/internal/stores"
)

// userPropsFromPacket keeps order and repeated keys.
func userPropsFromPacket(up []packets.UserProperty) []stores.UserProperty {
	if len(up) == 0 {
		return nil
	}
	out := make([]stores.UserProperty, len(up))
	for i, p := range up {
		out[i] = stores.UserProperty{Key: p.Key, Value: p.Val}
	}
	return out
}
