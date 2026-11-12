package floodgate

// Blocklist is the set of hosts that nothing should be queued for, held in memory for one fan-out
type Blocklist map[string]struct{}

// Contains returns TRUE if the host is dormant or paused
func (blocklist Blocklist) Contains(host string) bool {
	_, found := blocklist[host]
	return found
}
