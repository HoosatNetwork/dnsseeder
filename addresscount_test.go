package main

import (
	"net"
	"testing"

	"github.com/Hoosat-Oy/HTND/app/appmessage"
	"github.com/Hoosat-Oy/HTND/infrastructure/config"
)

// The crawler calls AddressCount while the manager goroutine prunes the
// address book; run with -race.
func TestAddressCountConcurrentWithPrune(t *testing.T) {
	activeConfig = &ConfigFlags{NetworkFlags: config.NetworkFlags{}}
	if err := activeConfig.NetworkFlags.ResolveNetwork(nil); err != nil {
		t.Fatalf("ResolveNetwork: %s", err)
	}
	peersDefaultPort = 42421
	m, err := NewManager(t.TempDir())
	if err != nil {
		t.Fatalf("NewManager: %s", err)
	}
	defer func() { close(m.quit); m.wg.Wait() }()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 500; i++ {
			ip := net.IPv4(8, 8, byte(i>>8), byte(i))
			m.AddAddresses([]*appmessage.NetAddress{appmessage.NewNetAddressIPPort(ip, uint16(peersDefaultPort))})
			m.Attempt(ip)
			m.prunePeers()
		}
	}()
	for i := 0; i < 500; i++ {
		m.AddressCount()
	}
	<-done
}
